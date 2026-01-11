import time

import aiohttp
import mediaflow_proxy.utils.http_utils
import orjson
from fastapi import APIRouter, Query, Request
from fastapi.responses import FileResponse, RedirectResponse

from comet.core.config_validation import config_check
from comet.core.models import database, settings, DebridConfig, VALID_DEBRID_SERVICES
from comet.debrid.manager import get_debrid
from comet.metadata.manager import MetadataScraper
from comet.services.streaming.manager import custom_handle_stream_request
from comet.utils.cache import NO_CACHE_HEADERS
from comet.utils.network import get_client_ip
from comet.utils.parsing import parse_optional_int

router = APIRouter()


def get_api_key_for_service(config: dict, service: str) -> str:
    """
    Get the API key for a specific debrid service from config.
    Supports both new multi-debrid format and legacy single debrid format.
    """
    # Check new multi-debrid configs
    debrid_configs = config.get("debridConfigs", [])
    for cfg in debrid_configs:
        if isinstance(cfg, dict) and cfg.get("service") == service:
            return cfg.get("apiKey", "")
        elif isinstance(cfg, DebridConfig) and cfg.service == service:
            return cfg.apiKey
    
    # Fallback to legacy single debrid config
    if config.get("debridService") == service:
        return config.get("debridApiKey", "")
    
    return ""


# New route with service parameter (multi-debrid)
@router.get(
    "/{b64config}/playback/{service}/{hash}/{index}/{season}/{episode}/{torrent_name:path}",
    tags=["Stremio"],
    summary="Playback Proxy (Multi-Debrid)",
    description="Proxies the playback request to the specified Debrid service.",
)
async def playback_with_service(
    request: Request,
    b64config: str,
    service: str,
    hash: str,
    index: str,
    season: str,
    episode: str,
    torrent_name: str,
    name_query: str = Query(None, alias="name"),
):
    # Validate service parameter - if it looks like a hash, redirect to legacy endpoint
    if service not in VALID_DEBRID_SERVICES:
        # This might be an old-format URL where service is actually the hash
        # Redirect to legacy handler
        return await playback_legacy(
            request, b64config, service, hash, index, season, episode, 
            torrent_name=f"{torrent_name}", name_query=name_query
        )
    
    config = config_check(b64config)
    
    season_int = parse_optional_int(season)
    episode_int = parse_optional_int(episode)
    
    # Get API key for the specified service
    api_key = get_api_key_for_service(config, service)
    if not api_key:
        return FileResponse(
            "comet/assets/uncached.mp4", headers=NO_CACHE_HEADERS
        )

    async with aiohttp.ClientSession() as session:
        cached_link = await database.fetch_one(
            """
            SELECT download_url
            FROM download_links_cache
            WHERE debrid_key = :debrid_key
            AND info_hash = :info_hash
            AND ((CAST(:season as INTEGER) IS NULL AND season IS NULL) OR season = CAST(:season as INTEGER))
            AND ((CAST(:episode as INTEGER) IS NULL AND episode IS NULL) OR episode = CAST(:episode as INTEGER))
            AND timestamp + 3600 >= :current_time
            """,
            {
                "debrid_key": api_key,
                "info_hash": hash,
                "season": season_int,
                "episode": episode_int,
                "current_time": time.time(),
            },
        )

        download_url = None
        if cached_link:
            download_url = cached_link["download_url"]

        ip = get_client_ip(request)
        should_proxy = (
            settings.PROXY_DEBRID_STREAM
            and settings.PROXY_DEBRID_STREAM_PASSWORD
            == config["debridStreamProxyPassword"]
        )

        if download_url is None:
            # Retrieve torrent sources from database for private trackers
            torrent_data = await database.fetch_one(
                """
                SELECT sources, media_id
                FROM torrents
                WHERE info_hash = :info_hash
                LIMIT 1
                """,
                {"info_hash": hash},
            )

            sources = []
            media_id = None
            if torrent_data:
                if torrent_data["sources"]:
                    sources = orjson.loads(torrent_data["sources"])
                media_id = torrent_data["media_id"]

            aliases = {}
            if media_id:
                metadata_scraper = MetadataScraper(session)
                media_type = "series" if season_int is not None else "movie"

                if "tt" in media_id:
                    full_media_id = (
                        f"{media_id}:{season_int}:{episode_int}"
                        if media_type == "series"
                        else media_id
                    )
                else:
                    full_media_id = (
                        f"kitsu:{media_id}:{episode_int}"
                        if media_type == "series"
                        else f"kitsu:{media_id}"
                    )

                _, aliases = await metadata_scraper.fetch_metadata_and_aliases(
                    media_type, full_media_id
                )

            debrid = get_debrid(
                session,
                None,
                None,
                service,
                api_key,
                ip if not should_proxy else "",
            )
            download_url = await debrid.generate_download_link(
                hash, index, name_query, torrent_name, season_int, episode_int, sources, aliases
            )
            if not download_url:
                return FileResponse(
                    "comet/assets/uncached.mp4", headers=NO_CACHE_HEADERS
                )

            await database.execute(
                f"""
                    INSERT {"OR IGNORE " if settings.DATABASE_TYPE == "sqlite" else ""}
                    INTO download_links_cache
                    VALUES (:debrid_key, :info_hash, :season, :episode, :download_url, :timestamp)
                    {" ON CONFLICT DO NOTHING" if settings.DATABASE_TYPE == "postgresql" else ""}
                """,
                {
                    "debrid_key": api_key,
                    "info_hash": hash,
                    "season": season_int,
                    "episode": episode_int,
                    "download_url": download_url,
                    "timestamp": time.time(),
                },
            )

        if should_proxy:
            return await custom_handle_stream_request(
                request.method,
                download_url,
                mediaflow_proxy.utils.http_utils.get_proxy_headers(request),
                media_id=torrent_name,
                ip=ip,
            )

        return RedirectResponse(download_url, status_code=302)


# Legacy route without service parameter (backward compatibility)
@router.get(
    "/{b64config}/playback/{hash}/{index}/{season}/{episode}/{torrent_name:path}",
    tags=["Stremio"],
    summary="Playback Proxy (Legacy)",
    description="Proxies the playback request to the Debrid service or returns a cached link.",
)
async def playback_legacy(
    request: Request,
    b64config: str,
    hash: str,
    index: str,
    season: str,
    episode: str,
    torrent_name: str,
    name_query: str = Query(None, alias="name"),
):
    config = config_check(b64config)

    season_int = parse_optional_int(season)
    episode_int = parse_optional_int(episode)
    
    # Get debrid service and API key (legacy or first from multi-debrid)
    debrid_configs = config.get("debridConfigs", [])
    if debrid_configs:
        first_config = debrid_configs[0]
        if isinstance(first_config, dict):
            debrid_service = first_config.get("service", config.get("debridService", "torrent"))
            api_key = first_config.get("apiKey", config.get("debridApiKey", ""))
        else:
            debrid_service = first_config.service
            api_key = first_config.apiKey
    else:
        debrid_service = config.get("debridService", "torrent")
        api_key = config.get("debridApiKey", "")

    async with aiohttp.ClientSession() as session:
        cached_link = await database.fetch_one(
            """
            SELECT download_url
            FROM download_links_cache
            WHERE debrid_key = :debrid_key
            AND info_hash = :info_hash
            AND ((CAST(:season as INTEGER) IS NULL AND season IS NULL) OR season = CAST(:season as INTEGER))
            AND ((CAST(:episode as INTEGER) IS NULL AND episode IS NULL) OR episode = CAST(:episode as INTEGER))
            AND timestamp + 3600 >= :current_time
            """,
            {
                "debrid_key": api_key,
                "info_hash": hash,
                "season": season_int,
                "episode": episode_int,
                "current_time": time.time(),
            },
        )

        download_url = None
        if cached_link:
            download_url = cached_link["download_url"]

        ip = get_client_ip(request)
        should_proxy = (
            settings.PROXY_DEBRID_STREAM
            and settings.PROXY_DEBRID_STREAM_PASSWORD
            == config["debridStreamProxyPassword"]
        )

        if download_url is None:
            # Retrieve torrent sources from database for private trackers
            torrent_data = await database.fetch_one(
                """
                SELECT sources, media_id
                FROM torrents
                WHERE info_hash = :info_hash
                LIMIT 1
                """,
                {"info_hash": hash},
            )

            sources = []
            media_id = None
            if torrent_data:
                if torrent_data["sources"]:
                    sources = orjson.loads(torrent_data["sources"])
                media_id = torrent_data["media_id"]

            aliases = {}
            if media_id:
                metadata_scraper = MetadataScraper(session)
                media_type = "series" if season_int is not None else "movie"

                if "tt" in media_id:
                    full_media_id = (
                        f"{media_id}:{season_int}:{episode_int}"
                        if media_type == "series"
                        else media_id
                    )
                else:
                    full_media_id = (
                        f"kitsu:{media_id}:{episode_int}"
                        if media_type == "series"
                        else f"kitsu:{media_id}"
                    )

                _, aliases = await metadata_scraper.fetch_metadata_and_aliases(
                    media_type, full_media_id
                )

            debrid = get_debrid(
                session,
                None,
                None,
                debrid_service,
                api_key,
                ip if not should_proxy else "",
            )
            download_url = await debrid.generate_download_link(
                hash, index, name_query, torrent_name, season_int, episode_int, sources, aliases
            )
            if not download_url:
                return FileResponse(
                    "comet/assets/uncached.mp4", headers=NO_CACHE_HEADERS
                )

            await database.execute(
                f"""
                    INSERT {"OR IGNORE " if settings.DATABASE_TYPE == "sqlite" else ""}
                    INTO download_links_cache
                    VALUES (:debrid_key, :info_hash, :season, :episode, :download_url, :timestamp)
                    {" ON CONFLICT DO NOTHING" if settings.DATABASE_TYPE == "postgresql" else ""}
                """,
                {
                    "debrid_key": api_key,
                    "info_hash": hash,
                    "season": season_int,
                    "episode": episode_int,
                    "download_url": download_url,
                    "timestamp": time.time(),
                },
            )

        if should_proxy:
            return await custom_handle_stream_request(
                request.method,
                download_url,
                mediaflow_proxy.utils.http_utils.get_proxy_headers(request),
                media_id=torrent_name,
                ip=ip,
            )

        return RedirectResponse(download_url, status_code=302)
