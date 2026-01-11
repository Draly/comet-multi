import base64

import orjson

from comet.core.models import (ConfigModel, DebridConfig, default_config,
                               rtn_ranking_default, rtn_settings_default,
                               settings)


def normalize_debrid_config(config: dict) -> dict:
    """
    Normalize config to ensure debridConfigs is populated.
    Handles backward compatibility with legacy single debrid format.
    """
    # If debridConfigs exists and is populated, use it
    debrid_configs = config.get("debridConfigs", [])
    
    if debrid_configs and len(debrid_configs) > 0:
        # Already has new format, just ensure proper structure
        normalized_configs = []
        for cfg in debrid_configs:
            if isinstance(cfg, dict):
                normalized_configs.append(cfg)
            elif isinstance(cfg, DebridConfig):
                normalized_configs.append({"service": cfg.service, "apiKey": cfg.apiKey})
        config["debridConfigs"] = normalized_configs
        
        # Also populate legacy fields for backward compatibility
        if normalized_configs:
            config["debridService"] = normalized_configs[0]["service"]
            config["debridApiKey"] = normalized_configs[0]["apiKey"]
    else:
        # Legacy format: convert single debrid to debridConfigs
        debrid_service = config.get("debridService", "torrent")
        debrid_api_key = config.get("debridApiKey", "")
        
        if debrid_service and debrid_service != "torrent" and debrid_api_key:
            config["debridConfigs"] = [{"service": debrid_service, "apiKey": debrid_api_key}]
        else:
            config["debridConfigs"] = []
    
    return config


def config_check(b64config: str):
    try:
        config = orjson.loads(base64.b64decode(b64config).decode())
        
        # Normalize debrid configuration before validation
        config = normalize_debrid_config(config)

        validated_config = ConfigModel(**config)
        validated_config = validated_config.model_dump()

        for key in list(validated_config["options"].keys()):
            if key not in [
                "remove_ranks_under",
                "allow_english_in_languages",
                "remove_unknown_languages",
            ]:
                validated_config["options"].pop(key)

        validated_config["options"]["remove_all_trash"] = validated_config[
            "removeTrash"
        ]

        rtn_settings = rtn_settings_default.model_copy(
            update={
                "resolutions": rtn_settings_default.resolutions.model_copy(
                    update=validated_config["resolutions"]
                ),
                "options": rtn_settings_default.options.model_copy(
                    update=validated_config["options"]
                ),
                "languages": rtn_settings_default.languages.model_copy(
                    update=validated_config["languages"]
                ),
            }
        )

        validated_config["rtnSettings"] = rtn_settings
        validated_config["rtnRanking"] = rtn_ranking_default

        # Handle proxy debrid stream defaults
        if (
            settings.PROXY_DEBRID_STREAM
            and settings.PROXY_DEBRID_STREAM_PASSWORD
            == validated_config["debridStreamProxyPassword"]
        ):
            # Check if we have no debrid configs at all
            has_debrid_configs = (
                validated_config.get("debridConfigs") 
                and len(validated_config["debridConfigs"]) > 0
            )
            has_legacy_key = validated_config.get("debridApiKey", "") != ""
            
            if not has_debrid_configs and not has_legacy_key:
                # Set defaults for proxy mode
                validated_config["debridService"] = (
                    settings.PROXY_DEBRID_STREAM_DEBRID_DEFAULT_SERVICE
                )
                validated_config["debridApiKey"] = (
                    settings.PROXY_DEBRID_STREAM_DEBRID_DEFAULT_APIKEY
                )
                # Also update debridConfigs
                if settings.PROXY_DEBRID_STREAM_DEBRID_DEFAULT_APIKEY:
                    validated_config["debridConfigs"] = [{
                        "service": settings.PROXY_DEBRID_STREAM_DEBRID_DEFAULT_SERVICE,
                        "apiKey": settings.PROXY_DEBRID_STREAM_DEBRID_DEFAULT_APIKEY
                    }]

        return validated_config
    except Exception:
        return default_config  # if it doesn't pass, return default config
