from __future__ import annotations

import asyncio
import json
import mimetypes
import random
import shutil
from pathlib import Path
from typing import Awaitable, Callable

from config import (
    API_HASH,
    API_ID,
    BOT_TOKEN,
    TELETHON_PARALLEL_UPLOAD,
    TELETHON_PARALLEL_UPLOAD_THRESHOLD_MB,
    TELETHON_PARALLEL_UPLOAD_WORKERS,
    TELETHON_SESSION_NAME,
)

_client = None
_enabled = False
_last_error = ""

ProgressCallback = Callable[[int, int], Awaitable[None] | None]
PARALLEL_THRESHOLD_BYTES = max(1, TELETHON_PARALLEL_UPLOAD_THRESHOLD_MB) * 1024 * 1024


def telethon_configured() -> bool:
    return bool(API_ID and API_HASH and BOT_TOKEN)


def last_telethon_error() -> str:
    if _last_error:
        return _last_error
    if not API_ID:
        return "API_ID nao configurado."
    if not API_HASH:
        return "API_HASH nao configurado."
    if not BOT_TOKEN:
        return "BOT_TOKEN nao configurado."
    if not _enabled or not _client:
        return "Uploader Telethon ainda nao esta conectado."
    return ""


def _as_message_list(result) -> list:
    if result is None:
        return []
    if isinstance(result, (list, tuple)):
        return [item for item in result if item is not None]
    return [result]


async def _delete_messages_best_effort(chat_id: int, messages: list) -> None:
    ids = [getattr(message, "id", None) for message in messages]
    ids = [message_id for message_id in ids if message_id is not None]
    if not ids:
        return
    try:
        await _client.delete_messages(chat_id, ids)
    except Exception:
        pass


async def _assert_protected(chat_id: int, result) -> None:
    messages = _as_message_list(result)
    if not messages:
        raise RuntimeError("Nao consegui confirmar se o episodio foi enviado protegido.")

    unprotected = [
        message
        for message in messages
        if getattr(message, "noforwards", False) is not True
    ]
    if not unprotected:
        return

    await _delete_messages_best_effort(chat_id, messages)
    raise RuntimeError(
        "O Telegram nao confirmou o bloqueio de compartilhamento nesse envio. "
        "Apaguei o envio para nao vazar desprotegido. Atualize o Telethon e tente de novo."
    )


async def start_telethon_uploader() -> bool:
    global _client, _enabled, _last_error
    if _enabled and _client:
        _last_error = ""
        return True

    if not telethon_configured():
        _last_error = last_telethon_error()
        return False

    try:
        from telethon import TelegramClient

        session_path = Path(TELETHON_SESSION_NAME)
        session_path.parent.mkdir(parents=True, exist_ok=True)

        _client = TelegramClient(str(session_path), API_ID, API_HASH)
        await _client.start(bot_token=BOT_TOKEN)
        _enabled = True
        _last_error = ""
        return True
    except Exception as error:
        _last_error = repr(error)
        print(f"[TELETHON_UPLOAD] disabled: {_last_error}")
        _client = None
        _enabled = False
        return False


async def stop_telethon_uploader() -> None:
    global _client, _enabled, _last_error
    if _client:
        try:
            await _client.disconnect()
        except Exception:
            pass
    _client = None
    _enabled = False
    _last_error = ""


async def _probe_video(path: Path) -> dict:
    ffprobe = shutil.which("ffprobe")
    if not ffprobe:
        return {}

    proc = await asyncio.create_subprocess_exec(
        ffprobe,
        "-v",
        "quiet",
        "-print_format",
        "json",
        "-show_format",
        "-show_streams",
        str(path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await proc.communicate()
    if proc.returncode != 0:
        return {}

    try:
        data = json.loads(stdout.decode("utf-8", errors="ignore"))
    except Exception:
        return {}

    video_stream = None
    for stream in data.get("streams") or []:
        if stream.get("codec_type") == "video":
            video_stream = stream
            break

    duration = 0
    try:
        duration = int(float((data.get("format") or {}).get("duration") or 0))
    except Exception:
        duration = 0

    return {
        "duration": duration,
        "width": int((video_stream or {}).get("width") or 0),
        "height": int((video_stream or {}).get("height") or 0),
    }


async def _make_thumbnail(path: Path) -> Path | None:
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        return None

    thumb = path.with_suffix(".thumb.jpg")
    proc = await asyncio.create_subprocess_exec(
        ffmpeg,
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-ss",
        "00:00:01.000",
        "-i",
        str(path),
        "-vframes",
        "1",
        "-vf",
        "scale='min(320,iw)':-2",
        str(thumb),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    await proc.communicate()
    if proc.returncode == 0 and thumb.exists() and thumb.stat().st_size > 0:
        return thumb
    thumb.unlink(missing_ok=True)
    return None


async def _emit_progress(callback: ProgressCallback | None, current: int, total: int) -> None:
    if not callback:
        return
    result = callback(current, total)
    if asyncio.iscoroutine(result):
        await result


async def _upload_big_file_parallel(path: Path, progress_callback: ProgressCallback | None):
    from telethon.tl.functions.upload import SaveBigFilePartRequest
    from telethon.tl.types import InputFileBig

    file_size = path.stat().st_size
    part_size = 512 * 1024
    total_parts = (file_size + part_size - 1) // part_size
    file_id = random.randrange(-(2**63), 2**63)
    workers = max(1, TELETHON_PARALLEL_UPLOAD_WORKERS)

    next_part = 0
    completed = 0
    part_lock = asyncio.Lock()
    progress_lock = asyncio.Lock()

    async def reserve_part() -> int | None:
        nonlocal next_part
        async with part_lock:
            if next_part >= total_parts:
                return None
            part = next_part
            next_part += 1
            return part

    def read_part(part: int) -> bytes:
        with path.open("rb") as file:
            file.seek(part * part_size)
            return file.read(part_size)

    async def worker() -> None:
        nonlocal completed
        while True:
            part = await reserve_part()
            if part is None:
                return
            chunk = await asyncio.to_thread(read_part, part)
            await _client(SaveBigFilePartRequest(file_id, part, total_parts, chunk))
            async with progress_lock:
                completed += len(chunk)
                await _emit_progress(progress_callback, min(completed, file_size), file_size)

    await asyncio.gather(*(worker() for _ in range(workers)))
    await _emit_progress(progress_callback, file_size, file_size)
    return InputFileBig(file_id, total_parts, path.name)


async def _send_media_request(
    chat_id: int,
    path: Path,
    caption: str,
    *,
    as_video: bool,
    attrs,
    thumb: Path | None,
    progress_callback: ProgressCallback | None,
    protect_content: bool,
):
    from telethon import functions, types

    file_size = path.stat().st_size
    file_arg = None
    callback = progress_callback

    if (
        TELETHON_PARALLEL_UPLOAD
        and as_video
        and file_size >= PARALLEL_THRESHOLD_BYTES
    ):
        file_arg = await _upload_big_file_parallel(path, progress_callback)
        callback = None
    else:
        file_arg = await _client.upload_file(
            str(path),
            file_size=file_size,
            file_name=path.name,
            progress_callback=callback,
        )

    thumb_arg = None
    if thumb:
        thumb_arg = await _client.upload_file(str(thumb), file_name=thumb.name)

    attributes = list(attrs or [])
    if not any(isinstance(attr, types.DocumentAttributeFilename) for attr in attributes):
        attributes.append(types.DocumentAttributeFilename(path.name))

    mime_type = mimetypes.guess_type(path.name)[0] or "video/mp4"
    media = types.InputMediaUploadedDocument(
        file=file_arg,
        mime_type=mime_type,
        attributes=attributes,
        force_file=not as_video,
        thumb=thumb_arg,
    )

    entity = await _client.get_input_entity(chat_id)
    parsed_caption, entities = await _client._parse_message_text(caption, "html")
    request = functions.messages.SendMediaRequest(
        peer=entity,
        media=media,
        message=parsed_caption,
        entities=entities,
        noforwards=protect_content,
    )
    result = await _client(request)
    return _client._get_response_message(request, result, entity)


async def send_file_with_telethon(
    chat_id: int,
    path: Path,
    caption: str,
    *,
    as_video: bool = True,
    progress_callback: ProgressCallback | None = None,
    protect_content: bool = True,
) -> bool:
    if not _enabled or not _client:
        ok = await start_telethon_uploader()
        if not ok:
            return False

    attrs = None
    thumb = None

    if as_video:
        try:
            from telethon.tl.types import DocumentAttributeVideo

            meta = await _probe_video(path)
            attrs = [
                DocumentAttributeVideo(
                    duration=int(meta.get("duration") or 0),
                    w=int(meta.get("width") or 0),
                    h=int(meta.get("height") or 0),
                    supports_streaming=True,
                )
            ]
        except Exception:
            attrs = None

        thumb = await _make_thumbnail(path)

    try:
        result = await _send_media_request(
            chat_id,
            path,
            caption,
            as_video=as_video,
            attrs=attrs,
            thumb=thumb,
            progress_callback=progress_callback,
            protect_content=protect_content,
        )
        if protect_content:
            await _assert_protected(chat_id, result)
    finally:
        if thumb:
            thumb.unlink(missing_ok=True)

    return True
