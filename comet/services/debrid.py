import asyncio
from typing import List

import orjson
from RTN import ParsedData

from comet.core.logger import logger
from comet.debrid.manager import retrieve_debrid_availability, get_debrid_extension
from comet.services.debrid_cache import (cache_availability,
                                         get_cached_availability)


class DebridService:
    def __init__(self, debrid_service: str, debrid_api_key: str, ip: str):
        self.debrid_service = debrid_service
        self.debrid_api_key = debrid_api_key
        self.ip = ip

    async def get_and_cache_availability(
        self,
        session,
        torrents: dict,
        media_id: str,
        media_only_id: str,
        season: int,
        episode: int,
    ):
        info_hashes = list(torrents.keys())

        seeders_map = {hash: torrents[hash]["seeders"] for hash in info_hashes}
        tracker_map = {hash: torrents[hash]["tracker"] for hash in info_hashes}
        sources_map = {hash: torrents[hash]["sources"] for hash in info_hashes}

        availability = await retrieve_debrid_availability(
            session,
            media_id,
            media_only_id,
            self.debrid_service,
            self.debrid_api_key,
            self.ip,
            info_hashes,
            seeders_map,
            tracker_map,
            sources_map,
        )

        if len(availability) == 0:
            return

        for file in availability:
            file_season = file["season"]
            file_episode = file["episode"]
            if (file_season is not None and file_season != season) or (
                file_episode is not None and file_episode != episode
            ):
                continue

            info_hash = file["info_hash"]
            if info_hash not in torrents:
                continue
            torrents[info_hash]["cached"] = True

            debrid_parsed = file["parsed"]
            if debrid_parsed is not None:
                if (
                    debrid_parsed.quality is None
                    and torrents[info_hash]["parsed"].quality is not None
                ):
                    debrid_parsed.quality = torrents[info_hash]["parsed"].quality
                torrents[info_hash]["parsed"] = debrid_parsed
            if file["index"] is not None:
                torrents[info_hash]["fileIndex"] = file["index"]
            if file["title"] is not None:
                torrents[info_hash]["title"] = file["title"]
            if file["size"] is not None:
                torrents[info_hash]["size"] = file["size"]

        asyncio.create_task(cache_availability(self.debrid_service, availability))

    async def check_existing_availability(
        self, torrents: dict, season: int, episode: int
    ):
        info_hashes = list(torrents.keys())
        for hash in info_hashes:
            torrents[hash]["cached"] = False

        if len(torrents) == 0:
            return

        rows = await get_cached_availability(
            self.debrid_service, info_hashes, season, episode
        )

        for row in rows:
            info_hash = row["info_hash"]
            torrents[info_hash]["cached"] = True

            if row["file_index"] is not None:
                try:
                    torrents[info_hash]["fileIndex"] = int(row["file_index"])
                except ValueError:
                    pass

            if row["size"] is not None:
                torrents[info_hash]["size"] = row["size"]

            # Only update title/parsed if the cached file has resolution info
            # Otherwise keep the original torrent info which may have better quality data
            # E.g. torrent "[Group] Show S01 1080p" vs file "Show - 02.mkv"
            if row["parsed"] is not None:
                cached_parsed = ParsedData(**orjson.loads(row["parsed"]))
                if (
                    cached_parsed.resolution != "unknown"
                    or torrents[info_hash]["parsed"].resolution == "unknown"
                ):
                    torrents[info_hash]["parsed"] = cached_parsed
                    if row["title"] is not None:
                        torrents[info_hash]["title"] = row["title"]


class MultiDebridService:
    """
    Service that handles multiple debrid services in parallel.
    Aggregates results from all configured debrid services.
    """

    def __init__(self, debrid_configs: List, ip: str):
        """
        Initialize with a list of DebridConfig objects.
        
        Args:
            debrid_configs: List of DebridConfig objects with service and apiKey
            ip: Client IP address
        """
        self.debrid_configs = debrid_configs
        self.ip = ip
        self.services = [
            DebridService(cfg.service, cfg.apiKey, ip)
            for cfg in debrid_configs
            if cfg.service != "torrent"
        ]

    def get_service_names(self) -> List[str]:
        """Returns list of service names."""
        return [cfg.service for cfg in self.debrid_configs if cfg.service != "torrent"]

    async def get_and_cache_availability_all(
        self,
        session,
        torrents: dict,
        media_id: str,
        media_only_id: str,
        season: int,
        episode: int,
    ):
        """
        Check availability on all configured debrid services in parallel.
        Updates torrents dict with cached_services list for each torrent.
        """
        if not self.services:
            return

        # Initialize cached_services for all torrents
        for info_hash in torrents:
            if "cached_services" not in torrents[info_hash]:
                torrents[info_hash]["cached_services"] = []

        info_hashes = list(torrents.keys())
        seeders_map = {hash: torrents[hash]["seeders"] for hash in info_hashes}
        tracker_map = {hash: torrents[hash]["tracker"] for hash in info_hashes}
        sources_map = {hash: torrents[hash]["sources"] for hash in info_hashes}

        # Create tasks for all debrid services
        async def check_service(service: DebridService):
            try:
                availability = await retrieve_debrid_availability(
                    session,
                    media_id,
                    media_only_id,
                    service.debrid_service,
                    service.debrid_api_key,
                    self.ip,
                    info_hashes,
                    seeders_map,
                    tracker_map,
                    sources_map,
                )
                return service.debrid_service, availability
            except Exception as e:
                logger.warning(
                    f"Failed to check availability on {service.debrid_service}: {e}"
                )
                return service.debrid_service, []

        # Run all checks in parallel
        tasks = [check_service(service) for service in self.services]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results and mark torrents with their cached services
        for result in results:
            if isinstance(result, Exception):
                logger.warning(f"Exception during availability check: {result}")
                continue

            service_name, availability = result
            if not availability:
                continue

            for file in availability:
                file_season = file["season"]
                file_episode = file["episode"]
                if (file_season is not None and file_season != season) or (
                    file_episode is not None and file_episode != episode
                ):
                    continue

                info_hash = file["info_hash"]
                if info_hash not in torrents:
                    continue

                # Mark as cached and add service to the list
                torrents[info_hash]["cached"] = True
                if service_name not in torrents[info_hash]["cached_services"]:
                    torrents[info_hash]["cached_services"].append(service_name)

                # Update torrent info from the first service that has it
                debrid_parsed = file["parsed"]
                if debrid_parsed is not None:
                    if (
                        debrid_parsed.quality is None
                        and torrents[info_hash]["parsed"].quality is not None
                    ):
                        debrid_parsed.quality = torrents[info_hash]["parsed"].quality
                    torrents[info_hash]["parsed"] = debrid_parsed
                if file["index"] is not None:
                    torrents[info_hash]["fileIndex"] = file["index"]
                if file["title"] is not None:
                    torrents[info_hash]["title"] = file["title"]
                if file["size"] is not None:
                    torrents[info_hash]["size"] = file["size"]

            # Cache the availability for this service
            asyncio.create_task(cache_availability(service_name, availability))

        # Log summary
        cached_count = sum(1 for t in torrents.values() if t.get("cached"))
        service_names = self.get_service_names()
        logger.log(
            "SCRAPER",
            f"💾 Multi-debrid check complete: {cached_count}/{len(torrents)} cached "
            f"across {len(service_names)} services: {', '.join(service_names)}",
        )

    async def check_existing_availability_all(
        self, torrents: dict, season: int, episode: int
    ):
        """
        Check existing cached availability across all configured debrid services.
        Updates torrents dict with cached_services list for each torrent.
        """
        info_hashes = list(torrents.keys())
        
        # Initialize all torrents
        for hash in info_hashes:
            torrents[hash]["cached"] = False
            torrents[hash]["cached_services"] = []

        if len(torrents) == 0 or not self.services:
            return

        # Check each service
        for service in self.services:
            rows = await get_cached_availability(
                service.debrid_service, info_hashes, season, episode
            )

            for row in rows:
                info_hash = row["info_hash"]
                if info_hash not in torrents:
                    continue

                torrents[info_hash]["cached"] = True
                if service.debrid_service not in torrents[info_hash]["cached_services"]:
                    torrents[info_hash]["cached_services"].append(service.debrid_service)

                if row["file_index"] is not None:
                    try:
                        torrents[info_hash]["fileIndex"] = int(row["file_index"])
                    except ValueError:
                        pass

                if row["size"] is not None:
                    torrents[info_hash]["size"] = row["size"]

                if row["parsed"] is not None:
                    cached_parsed = ParsedData(**orjson.loads(row["parsed"]))
                    if (
                        cached_parsed.resolution != "unknown"
                        or torrents[info_hash]["parsed"].resolution == "unknown"
                    ):
                        torrents[info_hash]["parsed"] = cached_parsed
                        if row["title"] is not None:
                            torrents[info_hash]["title"] = row["title"]

    def get_first_available_service(self, torrent: dict) -> str:
        """
        Get the first available debrid service for a torrent.
        Returns the service name or None if not cached.
        """
        cached_services = torrent.get("cached_services", [])
        if cached_services:
            return cached_services[0]
        return None

    def get_debrid_extension_for_torrent(self, torrent: dict) -> str:
        """
        Get the debrid extension string for display.
        Shows all services where the torrent is cached.
        """
        cached_services = torrent.get("cached_services", [])
        if not cached_services:
            # Return first configured service as fallback
            if self.debrid_configs:
                return get_debrid_extension(self.debrid_configs[0].service)
            return "?"
        
        # Join all service extensions
        extensions = [get_debrid_extension(svc) for svc in cached_services]
        return "/".join(extensions)

    def get_service_config(self, service_name: str):
        """Get the config for a specific service."""
        for cfg in self.debrid_configs:
            if cfg.service == service_name:
                return cfg
        return None
