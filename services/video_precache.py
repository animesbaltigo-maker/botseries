from __future__ import annotations

import asyncio
import html
import logging

from telegram.ext import ContextTypes

from config import VIDEO_PRECACHE_EPISODES_PER_TITLE, VIDEO_PRECACHE_TITLES
from core.video_download_queue import VideoDownloadJob, archive_video_if_missing, has_archived_video
from handlers.callbacks import (
    _audio_text_label,
    _best_download_server,
    _download_candidates,
    _episode_archive_caption,
    _episode_delivery_cache_key,
    _episode_display_label,
    _episode_number_value,
)
from services.catalog_client import get_content_details, get_player_links, get_season_episodes, search_content

LOGGER = logging.getLogger(__name__)


def _configured_titles() -> list[str]:
    return [item.strip() for item in VIDEO_PRECACHE_TITLES.split(",") if item.strip()]


async def _precache_title(app, query: str) -> int:
    results = await search_content(query)
    if not results:
        return 0
    item = results[0]
    detail_url = str(item.get("url") or item.get("detail_url") or "").strip()
    if not detail_url:
        return 0

    details = await get_content_details(detail_url)
    session = dict(item)
    session.update({k: v for k, v in details.items() if v})
    title = str(session.get("title") or item.get("title") or query).strip() or query
    selected_audio = str(session.get("selected_audio") or session.get("default_audio") or "dublado").strip().lower()
    episodes = await get_season_episodes(detail_url, 1)
    if not episodes:
        return 0

    archived_count = 0
    limit = max(0, int(VIDEO_PRECACHE_EPISODES_PER_TITLE or 0))
    for episode_idx, episode in enumerate(episodes[:limit]):
        try:
            player_links = await get_player_links(str(episode.get("url") or ""), preferred_audio=selected_audio)
            candidates = _download_candidates(player_links)
            player_url = candidates[0]["url"] if candidates else ""
            if not player_url:
                continue
            episode_number = _episode_number_value(episode, episode_idx)
            label = _episode_display_label(episode, episode_idx)
            item_label = f"T01E{episode_number:02d}"
            quality = _best_download_server(player_links, player_url)
            content_id = _episode_delivery_cache_key(session, 1, episode, episode_idx)
            if has_archived_video(content_id, item_label, quality):
                continue
            caption = _episode_archive_caption(title, label, 1, _audio_text_label(selected_audio), quality)
            archived = await archive_video_if_missing(
                app,
                VideoDownloadJob(
                    user_id=0,
                    chat_id=0,
                    content_id=content_id,
                    item_label=item_label,
                    quality=quality,
                    title=title,
                    video_url=player_url,
                    caption=caption,
                    video_urls=candidates,
                ),
            )
            archived_count += int(bool(archived))
            await asyncio.sleep(1.0)
        except Exception as error:
            LOGGER.warning("precache_failed title=%s episode=%s error=%r", title, episode_idx + 1, error)
    return archived_count


async def video_precache_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    titles = _configured_titles()
    if not titles:
        return
    total = 0
    for title in titles:
        try:
            total += await _precache_title(context.application, title)
        except Exception as error:
            LOGGER.warning("precache_title_failed query=%s error=%r", title, error)
    if total:
        LOGGER.info("video precache archived %s item(s)", total)
