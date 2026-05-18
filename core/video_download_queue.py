from __future__ import annotations

import asyncio
import html
import json
import re
import shutil
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from urllib.parse import urlparse

import httpx
from telegram.error import TelegramError, TimedOut

from config import (
    SOURCE_SITE_BASE,
    TELETHON_UPLOAD_MAX_MB,
    DOWNLOAD_ARCHIVE_CHANNEL,
    VIDEO_DOWNLOAD_ARIA2,
    VIDEO_CACHE_CLEANUP_INTERVAL_SECONDS,
    VIDEO_CACHE_TTL_HOURS,
    VIDEO_DOWNLOAD_CHUNK_MB,
    VIDEO_DOWNLOAD_CACHE_DIR,
    VIDEO_DOWNLOAD_MAX_MB,
    VIDEO_DOWNLOAD_PARALLEL,
    VIDEO_DOWNLOAD_PARALLEL_WORKERS,
    VIDEO_DOWNLOAD_PART_MB,
    VIDEO_DOWNLOAD_PROTECT_CONTENT,
    VIDEO_DOWNLOAD_TRUST_ENV,
    VIDEO_DOWNLOAD_QUEUE_LIMIT,
    VIDEO_DOWNLOAD_WORKERS,
    VIDEO_UPLOAD_MAX_MB,
)
from core.telethon_uploader import last_telethon_error, send_file_with_telethon, telethon_configured

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Referer": f"{SOURCE_SITE_BASE}/",
    "Origin": SOURCE_SITE_BASE,
}

CHUNK_SIZE = max(1, VIDEO_DOWNLOAD_CHUNK_MB) * 1024 * 1024
PART_SIZE = max(1, VIDEO_DOWNLOAD_PART_MB) * 1024 * 1024
PARALLEL_WORKERS = max(1, VIDEO_DOWNLOAD_PARALLEL_WORKERS)
PROGRESS_INTERVAL = 3.0
MAX_BYTES = max(1, VIDEO_DOWNLOAD_MAX_MB) * 1024 * 1024
UPLOAD_MAX_BYTES = max(1, VIDEO_UPLOAD_MAX_MB) * 1024 * 1024
TELETHON_MAX_BYTES = max(1, TELETHON_UPLOAD_MAX_MB) * 1024 * 1024
CACHE_TTL_SECONDS = max(1, VIDEO_CACHE_TTL_HOURS) * 3600
CACHE_CLEANUP_INTERVAL = max(60, VIDEO_CACHE_CLEANUP_INTERVAL_SECONDS)
PARTIAL_FILE_TTL_SECONDS = 15 * 60


@dataclass
class VideoDownloadJob:
    user_id: int
    chat_id: int
    content_id: str
    item_label: str
    quality: str
    title: str
    video_url: str
    caption: str
    video_urls: list[dict] = field(default_factory=list)


_workers: list[asyncio.Task] = []
_cleanup_task: asyncio.Task | None = None
_delivery_cleanup_task: asyncio.Task | None = None
_active_jobs: dict[str, dict] = {}
_active_user_jobs: dict[int, str] = {}
_enqueue_lock = asyncio.Lock()
_archive_lock = asyncio.Lock()

ARCHIVE_INDEX_PATH = Path(VIDEO_DOWNLOAD_CACHE_DIR).parent / "series_file_ids.json"
DELIVERY_INDEX_PATH = Path(VIDEO_DOWNLOAD_CACHE_DIR).parent / "series_delivery_messages.json"
DELIVERY_TTL_SECONDS = 24 * 60 * 60


def _job_key(content_id: str, item_label: str, quality: str) -> str:
    return f"{content_id}|{item_label}|{quality}".lower()


def _archive_chat_id() -> int | str:
    raw = str(DOWNLOAD_ARCHIVE_CHANNEL or "").strip()
    if raw.lstrip("-").isdigit():
        return int(raw)
    return raw


def _load_archive_index() -> dict:
    if not ARCHIVE_INDEX_PATH.exists():
        return {}
    try:
        data = json.loads(ARCHIVE_INDEX_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _save_archive_index(data: dict) -> None:
    ARCHIVE_INDEX_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = ARCHIVE_INDEX_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(ARCHIVE_INDEX_PATH)


def _delivery_key(user_id: int, content_id: str, item_label: str) -> str:
    return f"{int(user_id or 0)}|{str(content_id or '').strip()}|{str(item_label or '').strip()}".lower()


def _load_delivery_index() -> dict:
    if not DELIVERY_INDEX_PATH.exists():
        return {}
    try:
        data = json.loads(DELIVERY_INDEX_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _save_delivery_index(data: dict) -> None:
    DELIVERY_INDEX_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = DELIVERY_INDEX_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(DELIVERY_INDEX_PATH)


async def _delete_message_later(bot, chat_id: int, message_id: int, key: str) -> None:
    await asyncio.sleep(DELIVERY_TTL_SECONDS)
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception as error:
        print(f"[VIDEO_DELIVERY] delete_later_failed key={key} error={error!r}")
    data = _load_delivery_index()
    current = data.get(key) or {}
    if int(current.get("message_id") or 0) == int(message_id):
        data.pop(key, None)
        _save_delivery_index(data)


def _remember_delivered_video(bot, *, user_id: int, chat_id: int, sent, content_id: str, item_label: str) -> None:
    message_id = _message_id_from_sent(sent)
    if not message_id:
        return
    key = _delivery_key(user_id, content_id, item_label)
    data = _load_delivery_index()
    data[key] = {
        "chat_id": int(chat_id),
        "message_id": int(message_id),
        "user_id": int(user_id or 0),
        "content_id": str(content_id),
        "item_label": str(item_label),
        "created_at": int(time.time()),
        "expires_at": int(time.time()) + DELIVERY_TTL_SECONDS,
    }
    _save_delivery_index(data)
    asyncio.create_task(_delete_message_later(bot, int(chat_id), int(message_id), key))


async def delete_delivered_video_messages(bot, user_id: int, content_id: str, item_label: str) -> int:
    key = _delivery_key(user_id, content_id, item_label)
    data = _load_delivery_index()
    entry = data.pop(key, None)
    if not entry:
        return 0
    try:
        await bot.delete_message(chat_id=int(entry["chat_id"]), message_id=int(entry["message_id"]))
    except Exception as error:
        print(f"[VIDEO_DELIVERY] delete_seen_failed key={key} error={error!r}")
    _save_delivery_index(data)
    return 1


async def _delivery_cleanup_loop(bot) -> None:
    while True:
        try:
            now = int(time.time())
            data = _load_delivery_index()
            changed = False
            for key, entry in list(data.items()):
                if int(entry.get("expires_at") or 0) > now:
                    continue
                try:
                    await bot.delete_message(chat_id=int(entry["chat_id"]), message_id=int(entry["message_id"]))
                except Exception as error:
                    print(f"[VIDEO_DELIVERY] cleanup_delete_failed key={key} error={error!r}")
                data.pop(key, None)
                changed = True
            if changed:
                _save_delivery_index(data)
        except Exception as error:
            print(f"[VIDEO_DELIVERY] cleanup_error={error!r}")
        await asyncio.sleep(10 * 60)


def _message_id_from_sent(sent) -> int | None:
    if isinstance(sent, (list, tuple)) and sent:
        sent = sent[0]
    for attr in ("message_id", "id"):
        value = getattr(sent, attr, None)
        if value:
            try:
                return int(value)
            except Exception:
                return None
    return None


async def _copy_archived_video(bot, chat_id: int, entry: dict, caption: str):
    message_id = entry.get("archive_message_id")
    archive_chat_id = entry.get("archive_chat_id") or DOWNLOAD_ARCHIVE_CHANNEL
    if not message_id or not archive_chat_id:
        return False
    try:
        return await bot.copy_message(
            chat_id=chat_id,
            from_chat_id=archive_chat_id,
            message_id=int(message_id),
            caption=caption,
            parse_mode="HTML",
            protect_content=VIDEO_DOWNLOAD_PROTECT_CONTENT,
        )
    except Exception as error:
        print(f"[VIDEO_ARCHIVE] copy_failed key_entry={entry!r} error={error!r}")
        return None


async def _archive_downloaded_video(app, job: VideoDownloadJob, path: Path) -> dict:
    archive_chat_id = _archive_chat_id()
    if not archive_chat_id:
        return {}
    archive_caption = (
        f"{job.caption}\n\n"
        f"<code>{html.escape(_job_key(job.content_id, job.item_label, job.quality))}</code>"
    )
    sent = await _send_video_safe(app.bot, archive_chat_id, path, archive_caption, protect_content=False)
    message_id = _message_id_from_sent(sent)
    if not message_id:
        return {}
    return {
        "archive_chat_id": str(archive_chat_id),
        "archive_message_id": message_id,
        "content_id": job.content_id,
        "item_label": job.item_label,
        "quality": job.quality,
        "title": job.title,
        "caption": job.caption,
        "created_at": int(time.time()),
    }


def _safe_filename(value: str, fallback: str = "video") -> str:
    value = html.unescape(value or "")
    value = re.sub(r"[^\w\s.-]", " ", value, flags=re.UNICODE)
    value = re.sub(r"\s+", " ", value).strip(" .-_")
    return value[:120] or fallback


def _extension_from_url(url: str) -> str:
    path = urlparse(url or "").path.lower()
    if path.endswith(".m3u8"):
        return ".m3u8"
    if path.endswith(".webm"):
        return ".webm"
    if path.endswith(".mkv"):
        return ".mkv"
    return ".mp4"


def _is_hls_url(url: str) -> bool:
    return ".m3u8" in (url or "").lower()


def _human_size(value: int | None) -> str:
    if not value:
        return "0 MB"
    mb = value / (1024 * 1024)
    if mb < 1024:
        return f"{mb:.1f} MB"
    return f"{mb / 1024:.2f} GB"


def _parse_aria2_size(value: str) -> int | None:
    match = re.match(r"\s*([0-9.]+)\s*([KMGT]?i?B)?\s*$", value or "", flags=re.IGNORECASE)
    if not match:
        return None
    number = float(match.group(1))
    unit = (match.group(2) or "B").lower()
    multiplier = {
        "b": 1,
        "kb": 1000,
        "kib": 1024,
        "mb": 1000**2,
        "mib": 1024**2,
        "gb": 1000**3,
        "gib": 1024**3,
        "tb": 1000**4,
        "tib": 1024**4,
    }.get(unit, 1)
    return int(number * multiplier)


def _parse_aria2_progress(line: str, current: int, total: int | None) -> tuple[int, int | None]:
    match = re.search(
        r"(?P<current>[0-9.]+\s*[KMGT]?i?B)/(?P<total>[0-9.]+\s*[KMGT]?i?B)\((?P<pct>\d+)%\)",
        line,
        re.IGNORECASE,
    )
    if not match:
        return current, total
    return _parse_aria2_size(match.group("current")) or current, _parse_aria2_size(match.group("total")) or total


def _progress_bar(done: int, total: int | None, width: int = 10) -> str:
    if not total:
        return "\u25aa\ufe0f" * width
    ratio = max(0.0, min(1.0, done / max(total, 1)))
    filled = int(ratio * width)
    return ("\u25aa\ufe0f" * filled) + ("\u25ab\ufe0f" * (width - filled))


def _raise_if_too_large_for_upload(size: int) -> None:
    if size <= UPLOAD_MAX_BYTES:
        return
    if telethon_configured() and size <= TELETHON_MAX_BYTES:
        return
    raise RuntimeError(
        "O video foi encontrado, mas ficou grande demais para enviar pelo Bot API oficial.\n"
        f"Tamanho: {_human_size(size)}\n"
        f"Limite configurado: {_human_size(UPLOAD_MAX_BYTES)}\n\n"
        "Configure API_ID e API_HASH para ativar o uploader Telethon igual o bot de animes."
    )


def _cache_dir() -> Path:
    cache_dir = Path(VIDEO_DOWNLOAD_CACHE_DIR)
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def _cleanup_video_cache_sync() -> int:
    cache_dir = _cache_dir()
    now = time.time()
    removed = 0
    suffixes = {".mp4", ".mkv", ".webm", ".m3u8", ".part", ".jpg", ".jpeg"}
    for path in cache_dir.iterdir():
        try:
            if not path.is_file() or path.suffix.lower() not in suffixes:
                continue
            age = now - path.stat().st_mtime
            ttl = PARTIAL_FILE_TTL_SECONDS if path.name.endswith(".part") else CACHE_TTL_SECONDS
            if age >= ttl:
                path.unlink(missing_ok=True)
                removed += 1
        except Exception:
            pass
    return removed


async def cleanup_video_cache() -> int:
    return await asyncio.to_thread(_cleanup_video_cache_sync)


async def _cleanup_loop() -> None:
    while True:
        try:
            await cleanup_video_cache()
        except Exception as error:
            print(f"[VIDEO_CACHE] cleanup_error={error!r}")
        await asyncio.sleep(CACHE_CLEANUP_INTERVAL)


async def _safe_edit(message, text: str) -> None:
    try:
        await message.edit_text(text, parse_mode="HTML")
    except Exception:
        pass


async def _progress(entry: dict, job: VideoDownloadJob, downloaded: int, total: int | None) -> None:
    total_text = _human_size(total) if total else "calculando"
    pct = int((downloaded / total) * 100) if total else 0
    text = (
        "<b>Baixando video</b>\n\n"
        f"<b>Titulo:</b> {html.escape(job.title)}\n"
        f"<b>Item:</b> {html.escape(str(job.item_label))}\n"
        f"<b>Servidor:</b> {html.escape(job.quality)}\n"
        f"<b>Progresso:</b> {pct}%\n"
        f"{_progress_bar(downloaded, total)}\n"
        f"<code>{_human_size(downloaded)} / {total_text}</code>"
    )
    for message in list(entry["status_messages"]):
        await _safe_edit(message, text)


async def _upload_progress(entry: dict, job: VideoDownloadJob, current: int, total: int) -> None:
    pct = int((current / max(total, 1)) * 100)
    text = (
        "<b>Enviando video</b>\n\n"
        f"<b>Titulo:</b> {html.escape(job.title)}\n"
        f"<b>Item:</b> {html.escape(str(job.item_label))}\n"
        f"<b>Progresso:</b> {pct}%\n"
        f"{_progress_bar(current, total)}\n"
        f"<code>{_human_size(current)} / {_human_size(total)}</code>"
    )
    for message in list(entry["status_messages"]):
        await _safe_edit(message, text)


def _job_video_candidates(job: VideoDownloadJob) -> list[dict]:
    candidates = [{"url": job.video_url, "label": job.quality}]
    for item in job.video_urls or []:
        if not isinstance(item, dict):
            continue
        candidates.append({
            "url": str(item.get("url") or "").strip(),
            "label": str(item.get("label") or job.quality).strip() or job.quality,
        })

    unique = []
    seen = set()
    for item in candidates:
        url = str(item.get("url") or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        unique.append({"url": url, "label": str(item.get("label") or job.quality).strip() or job.quality})
    return unique


def _should_try_next_download_error(error: Exception) -> bool:
    text = str(error or "").lower()
    return any(
        token in text
        for token in (
            "player ainda nao fornece",
            "link de video nao pode",
            "response",
            "status code",
            "403",
            "404",
            "410",
            "429",
            "html",
        )
    )


async def _download_file(job: VideoDownloadJob, entry: dict) -> Path:
    last_error: Exception | None = None
    for candidate in _job_video_candidates(job):
        attempt = replace(job, video_url=candidate["url"], quality=candidate["label"])
        try:
            return await _download_single_file(attempt, entry)
        except Exception as error:
            last_error = error
            if not _should_try_next_download_error(error):
                raise
            print(f"[VIDEO_DOWNLOAD] player_failed label={candidate['label']!r} error={error!r}")

    if last_error:
        raise last_error
    raise RuntimeError("Nenhum player forneceu um arquivo de video direto.")


async def _download_single_file(job: VideoDownloadJob, entry: dict) -> Path:
    if not (job.video_url or "").lower().startswith("http"):
        raise RuntimeError("Esse link de video nao pode ser baixado direto.")

    cache_dir = _cache_dir()
    filename = _safe_filename(f"{job.title} - {job.item_label} - {job.quality}")
    target = cache_dir / f"{filename}{'.mp4' if _is_hls_url(job.video_url) else _extension_from_url(job.video_url)}"
    temp = cache_dir / f"{target.name}.part"

    if target.exists() and target.stat().st_size > 0:
        if time.time() - target.stat().st_mtime < CACHE_TTL_SECONDS:
            return target
        target.unlink(missing_ok=True)

    if _is_hls_url(job.video_url):
        return await _download_hls(job, entry, target, temp)

    timeout = httpx.Timeout(connect=10.0, read=60.0, write=60.0, pool=10.0)
    async with httpx.AsyncClient(
        timeout=timeout,
        follow_redirects=True,
        headers=HEADERS,
        trust_env=VIDEO_DOWNLOAD_TRUST_ENV,
    ) as client:
        probe = await _probe_direct_download(client, job.video_url)
        total = probe["total"]
        if total and total > MAX_BYTES:
            raise RuntimeError(f"Arquivo muito grande para enviar: {_human_size(total)}.")
        if total:
            _raise_if_too_large_for_upload(total)

        aria2c = shutil.which("aria2c")
        if VIDEO_DOWNLOAD_ARIA2 and aria2c and total and total >= PART_SIZE * 2:
            try:
                await _download_file_aria2(aria2c, job, entry, temp, total)
            except Exception as error:
                print(f"[VIDEO_DOWNLOAD] aria2_fallback error={error!r}")
                temp.unlink(missing_ok=True)
                if VIDEO_DOWNLOAD_PARALLEL and probe["range"] and total and total >= PART_SIZE * 2:
                    await _download_file_parallel(client, job, entry, temp, total)
                else:
                    await _download_file_stream(client, job, entry, temp, total)
        elif VIDEO_DOWNLOAD_PARALLEL and probe["range"] and total and total >= PART_SIZE * 2:
            await _download_file_parallel(client, job, entry, temp, total)
        else:
            await _download_file_stream(client, job, entry, temp, total)

    temp.replace(target)
    _raise_if_too_large_for_upload(target.stat().st_size)
    return target


async def _probe_direct_download(client: httpx.AsyncClient, url: str) -> dict:
    headers = dict(HEADERS)
    headers["Range"] = "bytes=0-0"
    async with client.stream("GET", url, headers=headers) as response:
        response.raise_for_status()

        content_type = str(response.headers.get("content-type") or "").lower()
        if content_type and "text/html" in content_type:
            raise RuntimeError("Esse player ainda nao fornece um arquivo de video direto.")

        content_range = str(response.headers.get("content-range") or "")
        total = 0
        match = re.search(r"/(\d+)\s*$", content_range)
        if match:
            total = int(match.group(1))
        else:
            total = int(response.headers.get("content-length") or 0)

        accepts_range = response.status_code == 206 or "bytes" in str(response.headers.get("accept-ranges") or "").lower()
        return {"total": total or None, "range": bool(accepts_range and total)}


async def _download_file_stream(
    client: httpx.AsyncClient,
    job: VideoDownloadJob,
    entry: dict,
    temp: Path,
    total: int | None,
) -> None:
    downloaded = 0
    last_progress = 0.0
    async with client.stream("GET", job.video_url) as response:
        response.raise_for_status()
        content_type = str(response.headers.get("content-type") or "").lower()
        if content_type and "text/html" in content_type:
            raise RuntimeError("Esse player ainda nao fornece um arquivo de video direto.")
        if total is None:
            total = int(response.headers.get("content-length") or 0) or None

        with open(temp, "wb") as file:
            async for chunk in response.aiter_bytes(CHUNK_SIZE):
                if not chunk:
                    continue
                downloaded += len(chunk)
                if downloaded > MAX_BYTES:
                    raise RuntimeError(f"Arquivo passou do limite de {_human_size(MAX_BYTES)}.")
                file.write(chunk)

                now = time.monotonic()
                if now - last_progress >= PROGRESS_INTERVAL:
                    last_progress = now
                    await _progress(entry, job, downloaded, total)


async def _download_file_parallel(
    client: httpx.AsyncClient,
    job: VideoDownloadJob,
    entry: dict,
    temp: Path,
    total: int,
) -> None:
    await _progress(entry, job, 0, total)
    temp.parent.mkdir(parents=True, exist_ok=True)
    with open(temp, "wb") as file:
        file.truncate(total)

    ranges = [(start, min(start + PART_SIZE - 1, total - 1)) for start in range(0, total, PART_SIZE)]
    semaphore = asyncio.Semaphore(min(PARALLEL_WORKERS, len(ranges)))

    downloaded = 0
    last_progress = 0.0
    progress_lock = asyncio.Lock()
    file_lock = asyncio.Lock()

    async def save_part(start: int, data: bytes) -> None:
        async with file_lock:
            with open(temp, "r+b") as file:
                file.seek(start)
                file.write(data)

    async def mark_progress(size: int) -> None:
        nonlocal downloaded, last_progress
        async with progress_lock:
            downloaded += size
            now = time.monotonic()
            if downloaded >= total or now - last_progress >= PROGRESS_INTERVAL:
                last_progress = now
                await _progress(entry, job, downloaded, total)

    async def download_part(start: int, end: int) -> None:
        async with semaphore:
            headers = dict(HEADERS)
            headers["Range"] = f"bytes={start}-{end}"
            response = await client.get(job.video_url, headers=headers)
            response.raise_for_status()
            if response.status_code != 206:
                raise RuntimeError("O servidor nao manteve download por partes.")
            data = response.content
            expected = end - start + 1
            if len(data) != expected:
                raise RuntimeError("Uma parte do download veio incompleta.")
            await save_part(start, data)
            await mark_progress(len(data))

    await asyncio.gather(*(download_part(start, end) for start, end in ranges))

    if temp.stat().st_size != total:
        raise RuntimeError("O download terminou com tamanho diferente do esperado.")


async def _download_file_aria2(aria2c: str, job: VideoDownloadJob, entry: dict, temp: Path, total: int | None) -> None:
    await _progress(entry, job, 0, total)
    temp.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        aria2c,
        "--allow-overwrite=true",
        "--auto-file-renaming=false",
        "--continue=true",
        "--file-allocation=none",
        "--summary-interval=1",
        "--console-log-level=error",
        "--show-console-readout=true",
        f"--max-connection-per-server={max(1, PARALLEL_WORKERS)}",
        f"--split={max(1, PARALLEL_WORKERS)}",
        f"--min-split-size={max(1, VIDEO_DOWNLOAD_PART_MB)}M",
        "--max-tries=3",
        "--retry-wait=3",
        f"--user-agent={HEADERS['User-Agent']}",
        f"--referer={HEADERS['Referer']}",
        "--dir",
        str(temp.parent),
        "--out",
        temp.name,
        job.video_url,
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    current = 0
    try:
        assert proc.stdout is not None
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            current, total = _parse_aria2_progress(line.decode("utf-8", errors="ignore"), current, total)
            if current:
                await _progress(entry, job, current, total)
        code = await proc.wait()
    except asyncio.CancelledError:
        proc.kill()
        await proc.wait()
        temp.unlink(missing_ok=True)
        raise
    if code != 0 or not temp.exists():
        temp.unlink(missing_ok=True)
        raise RuntimeError("O aria2c nao conseguiu concluir o download.")
    size = temp.stat().st_size
    if size > MAX_BYTES:
        temp.unlink(missing_ok=True)
        raise RuntimeError(f"Arquivo passou do limite de {_human_size(MAX_BYTES)}.")


async def _download_hls(job: VideoDownloadJob, entry: dict, target: Path, temp: Path) -> Path:
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        raise RuntimeError("Esse video veio em stream HLS. Instale ffmpeg no servidor para baixar offline.")
    await _progress(entry, job, 0, None)
    headers = (
        f"User-Agent: {HEADERS['User-Agent']}\r\n"
        f"Referer: {HEADERS['Referer']}\r\n"
        f"Origin: {HEADERS['Origin']}\r\n"
    )
    proc = await asyncio.create_subprocess_exec(
        ffmpeg, "-y", "-hide_banner", "-loglevel", "error",
        "-headers", headers, "-i", job.video_url,
        "-c", "copy", "-bsf:a", "aac_adtstoasc", "-f", "mp4", str(temp),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        message = stderr.decode("utf-8", errors="ignore").strip()[-500:]
        raise RuntimeError(message or "O ffmpeg nao conseguiu baixar esse stream.")
    if not temp.exists() or temp.stat().st_size <= 0:
        raise RuntimeError("O download terminou sem gerar arquivo.")
    if temp.stat().st_size > MAX_BYTES:
        temp.unlink(missing_ok=True)
        raise RuntimeError(f"Arquivo passou do limite de {_human_size(MAX_BYTES)}.")
    temp.replace(target)
    _raise_if_too_large_for_upload(target.stat().st_size)
    return target


async def _delete_downloaded_file(path: Path | None) -> None:
    if not path:
        return
    try:
        cache_dir = _cache_dir().resolve()
        target = path.resolve()
        if target.parent != cache_dir:
            return
        target.unlink(missing_ok=True)
        target.with_suffix(".thumb.jpg").unlink(missing_ok=True)
    except Exception as error:
        print(f"[VIDEO_CACHE] delete_downloaded_error={error!r}")


async def _send_video_safe(
    bot,
    chat_id: int,
    path: Path,
    caption: str,
    progress_cb=None,
    *,
    protect_content: bool | None = None,
) -> bool:
    size = path.stat().st_size
    effective_protect = VIDEO_DOWNLOAD_PROTECT_CONTENT if protect_content is None else bool(protect_content)
    if telethon_configured():
        if size > TELETHON_MAX_BYTES:
            raise RuntimeError(f"Arquivo maior que o limite Telethon configurado: {_human_size(size)} > {_human_size(TELETHON_MAX_BYTES)}.")
        sent = await send_file_with_telethon(
            chat_id,
            path,
            caption,
            as_video=True,
            progress_callback=progress_cb,
            protect_content=effective_protect,
        )
        if sent:
            return sent
        if size > UPLOAD_MAX_BYTES:
            reason = last_telethon_error() or "erro desconhecido"
            raise RuntimeError(
                "Arquivo grande demais para Bot API e o uploader Telethon nao conseguiu iniciar.\n"
                f"Motivo: {reason}"
            )

    if size > UPLOAD_MAX_BYTES:
        reason = last_telethon_error() or "API_ID/API_HASH ausentes"
        raise RuntimeError(
            "Arquivo grande demais para Bot API e o uploader Telethon nao esta configurado.\n"
            f"Motivo: {reason}"
        )

    try:
        with open(path, "rb") as file:
            sent = await bot.send_video(
                chat_id=chat_id,
                video=file,
                filename=path.name,
                caption=caption,
                parse_mode="HTML",
                supports_streaming=True,
                protect_content=effective_protect,
                read_timeout=120,
                write_timeout=120,
                connect_timeout=30,
                pool_timeout=30,
            )
        return sent
    except TimedOut:
        try:
            await bot.send_message(chat_id, "O envio demorou mais que o esperado. Confere se o video ja chegou.")
        except Exception:
            pass
        return True
    except TelegramError as error:
        if "request entity too large" in str(error).lower():
            raise RuntimeError("O Telegram recusou o upload porque o arquivo e grande demais para o Bot API oficial.") from error
        with open(path, "rb") as file:
            sent = await bot.send_document(
                chat_id=chat_id,
                document=file,
                filename=path.name,
                caption=caption,
                parse_mode="HTML",
                protect_content=effective_protect,
                read_timeout=120,
                write_timeout=120,
                connect_timeout=30,
                pool_timeout=30,
            )
        return sent


async def _process_job(app, job: VideoDownloadJob) -> None:
    key = _job_key(job.content_id, job.item_label, job.quality)
    entry = _active_jobs.get(key)
    if not entry:
        return
    path = None
    started_at = time.monotonic()
    downloaded_at = started_at
    archived_at = started_at
    try:
        await _progress(entry, job, 0, None)
        path = await _download_file(job, entry)
        downloaded_at = time.monotonic()
        for message in list(entry["status_messages"]):
            await _safe_edit(
                message,
                (
                    "<b>Enviando video</b>\n\n"
                    f"<b>Titulo:</b> {html.escape(job.title)}\n"
                    f"<b>Item:</b> {html.escape(str(job.item_label))}\n"
                    f"<b>Tamanho:</b> {_human_size(path.stat().st_size)}"
                ),
            )
        archive_entry = {}
        async with _archive_lock:
            index = _load_archive_index()
            archive_entry = index.get(key) or {}
            if not archive_entry:
                archive_entry = await _archive_downloaded_video(app, job, path)
                if archive_entry:
                    index[key] = archive_entry
                    _save_archive_index(index)
        archived_at = time.monotonic()

        for waiter in entry["waiters"]:
            last_upload_update = 0.0

            async def progress_cb(current: int, total: int):
                nonlocal last_upload_update
                now = time.monotonic()
                if current < total and now - last_upload_update < PROGRESS_INTERVAL:
                    return
                last_upload_update = now
                await _upload_progress(entry, job, current, total)

            sent = None
            if archive_entry:
                sent = await _copy_archived_video(app.bot, waiter["chat_id"], archive_entry, waiter["caption"])
            if not sent:
                sent = await _send_video_safe(app.bot, waiter["chat_id"], path, waiter["caption"], progress_cb=progress_cb)
            _remember_delivered_video(
                app.bot,
                user_id=waiter["user_id"],
                chat_id=waiter["chat_id"],
                sent=sent,
                content_id=job.content_id,
                item_label=job.item_label,
            )
        delivered_at = time.monotonic()
        print(
            "[VIDEO_TIMING] "
            f"key={key} size={path.stat().st_size if path else 0} "
            f"download={downloaded_at - started_at:.1f}s "
            f"archive={archived_at - downloaded_at:.1f}s "
            f"deliver={delivered_at - archived_at:.1f}s "
            f"total={delivered_at - started_at:.1f}s"
        )

        for message in list(entry["status_messages"]):
            await _safe_edit(
                message,
                (
                    "<b>Video enviado</b>\n\n"
                    f"<b>Titulo:</b> {html.escape(job.title)}\n"
                    f"<b>Item:</b> {html.escape(str(job.item_label))}"
                ),
            )
    except Exception as error:
        for message in list(entry["status_messages"]):
            await _safe_edit(message, f"<b>Falha ao baixar video:</b>\n<code>{html.escape(str(error))}</code>")
    finally:
        await _delete_downloaded_file(path)
        try:
            await cleanup_video_cache()
        except Exception:
            pass
        async with _enqueue_lock:
            _active_jobs.pop(key, None)
            for user_id, active_key in list(_active_user_jobs.items()):
                if active_key == key:
                    _active_user_jobs.pop(user_id, None)


async def _worker(app, queue: asyncio.Queue) -> None:
    while True:
        job = await queue.get()
        try:
            if job is None:
                return
            await _process_job(app, job)
        finally:
            queue.task_done()


async def enqueue_video_download(app, job: VideoDownloadJob) -> int:
    queue = app.bot_data["video_download_queue"]
    key = _job_key(job.content_id, job.item_label, job.quality)
    archive_entry = _load_archive_index().get(key) or {}
    if archive_entry:
        status = await app.bot.send_message(
            job.chat_id,
            (
                "<b>Enviando video</b>\n\n"
                f"<b>Titulo:</b> {html.escape(job.title)}\n"
                f"<b>Item:</b> {html.escape(str(job.item_label))}\n"
                "Status: <b>encontrado no acervo</b>"
            ),
            parse_mode="HTML",
        )
        sent = await _copy_archived_video(app.bot, job.chat_id, archive_entry, job.caption)
        if not sent:
            try:
                index = _load_archive_index()
                index.pop(key, None)
                _save_archive_index(index)
            except Exception:
                pass
        else:
            _remember_delivered_video(
                app.bot,
                user_id=job.user_id,
                chat_id=job.chat_id,
                sent=sent,
                content_id=job.content_id,
                item_label=job.item_label,
            )
            await _safe_edit(
                status,
                (
                    "<b>Video enviado</b>\n\n"
                    f"<b>Titulo:</b> {html.escape(job.title)}\n"
                    f"<b>Item:</b> {html.escape(str(job.item_label))}"
                ),
            )
            return queue.qsize()

    async with _enqueue_lock:
        if job.user_id in _active_user_jobs:
            raise RuntimeError("Voce ja tem um video em download ou upload. Aguarde terminar para pedir outro.")
        if key in _active_jobs:
            entry = _active_jobs[key]
            _active_user_jobs[job.user_id] = key
            status = await app.bot.send_message(
                job.chat_id,
                (
                    "<b>Pedido recebido</b>\n\n"
                    f"<b>Titulo:</b> {html.escape(job.title)}\n"
                    f"<b>Item:</b> {html.escape(str(job.item_label))}\n"
                    "Status: <b>ja esta sendo preparado</b>"
                ),
                parse_mode="HTML",
            )
            entry["waiters"].append({"user_id": job.user_id, "chat_id": job.chat_id, "caption": job.caption})
            entry["status_messages"].append(status)
            return queue.qsize()
        if queue.full():
            raise RuntimeError("A fila de downloads esta cheia agora. Tente de novo em alguns minutos.")
        _active_user_jobs[job.user_id] = key
        try:
            status = await app.bot.send_message(
                job.chat_id,
                (
                    "<b>Pedido recebido</b>\n\n"
                    f"<b>Titulo:</b> {html.escape(job.title)}\n"
                    f"<b>Item:</b> {html.escape(str(job.item_label))}\n"
                    "Status: <b>na fila</b>"
                ),
                parse_mode="HTML",
            )
            _active_jobs[key] = {
                "waiters": [{"user_id": job.user_id, "chat_id": job.chat_id, "caption": job.caption}],
                "status_messages": [status],
            }
            queue.put_nowait(job)
        except Exception:
            _active_user_jobs.pop(job.user_id, None)
            _active_jobs.pop(key, None)
            raise
        return queue.qsize()


async def start_video_download_workers(app) -> None:
    global _cleanup_task, _delivery_cleanup_task
    if app.bot_data.get("video_download_workers_started"):
        return
    await cleanup_video_cache()
    app.bot_data["video_download_queue"] = asyncio.Queue(maxsize=VIDEO_DOWNLOAD_QUEUE_LIMIT)
    for _ in range(max(1, VIDEO_DOWNLOAD_WORKERS)):
        _workers.append(asyncio.create_task(_worker(app, app.bot_data["video_download_queue"])))
    _cleanup_task = asyncio.create_task(_cleanup_loop())
    _delivery_cleanup_task = asyncio.create_task(_delivery_cleanup_loop(app.bot))
    app.bot_data["video_download_workers_started"] = True


async def stop_video_download_workers(app) -> None:
    global _cleanup_task, _delivery_cleanup_task
    queue = app.bot_data.get("video_download_queue")
    if queue is None:
        return
    for _ in _workers:
        await queue.put(None)
    await asyncio.gather(*_workers, return_exceptions=True)
    _workers.clear()
    if _cleanup_task:
        _cleanup_task.cancel()
        await asyncio.gather(_cleanup_task, return_exceptions=True)
        _cleanup_task = None
    if _delivery_cleanup_task:
        _delivery_cleanup_task.cancel()
        await asyncio.gather(_delivery_cleanup_task, return_exceptions=True)
        _delivery_cleanup_task = None
    await cleanup_video_cache()
    app.bot_data["video_download_workers_started"] = False
