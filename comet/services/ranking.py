from RTN import Torrent, check_fetch, get_rank, sort_torrents


def check_required_languages(parsed_languages: list, required_languages: list) -> bool:
    """
    Check if the torrent has at least one of the required languages.
    Returns True if no required languages are specified, or if at least one matches.
    
    Args:
        parsed_languages: List of language codes detected in the torrent (e.g., ['fr', 'en'])
        required_languages: List of required language codes or names (e.g., ['french'] or ['fr'])
    
    Returns:
        True if the torrent should be included, False otherwise
    """
    if not required_languages:
        return True
    
    if not parsed_languages:
        return False
    
    # Normalize to lowercase for comparison
    parsed_lower = [lang.lower() for lang in parsed_languages]
    
    # Map of language names to ISO codes for matching
    name_to_iso = {
        "multi": "multi",
        "english": "en",
        "japanese": "ja",
        "chinese": "zh",
        "russian": "ru",
        "arabic": "ar",
        "portuguese": "pt",
        "spanish": "es",
        "french": "fr",
        "german": "de",
        "italian": "it",
        "korean": "ko",
        "hindi": "hi",
        "bengali": "bn",
        "punjabi": "pa",
        "marathi": "mr",
        "gujarati": "gu",
        "tamil": "ta",
        "telugu": "te",
        "kannada": "kn",
        "malayalam": "ml",
        "thai": "th",
        "vietnamese": "vi",
        "indonesian": "id",
        "turkish": "tr",
        "hebrew": "he",
        "persian": "fa",
        "ukrainian": "uk",
        "greek": "el",
        "lithuanian": "lt",
        "latvian": "lv",
        "estonian": "et",
        "polish": "pl",
        "czech": "cs",
        "slovak": "sk",
        "hungarian": "hu",
        "romanian": "ro",
        "bulgarian": "bg",
        "serbian": "sr",
        "croatian": "hr",
        "slovenian": "sl",
        "dutch": "nl",
        "danish": "da",
        "finnish": "fi",
        "swedish": "sv",
        "norwegian": "no",
        "malay": "ms",
        "latino": "la",
    }
    
    for req_lang in required_languages:
        req_lower = req_lang.lower()
        # Check if it's a language name and convert to ISO
        iso_code = name_to_iso.get(req_lower, req_lower)
        
        if iso_code in parsed_lower:
            return True
    
    return False


def rank_worker(
    torrents,
    debrid_service,
    rtn_settings,
    rtn_ranking,
    max_results_per_resolution,
    max_size,
    cached_only,
    remove_trash,
):
    ranked_torrents = set()
    
    # Get required languages from settings
    required_languages = []
    if hasattr(rtn_settings, 'languages') and hasattr(rtn_settings.languages, 'required'):
        required_languages = rtn_settings.languages.required or []
    
    # Debug counters
    excluded_by_language = 0
    included_count = 0
    
    for info_hash, torrent in torrents.items():
        if cached_only and debrid_service != "torrent" and not torrent["cached"]:
            continue

        if max_size != 0 and torrent["size"] > max_size:
            continue

        parsed = torrent["parsed"]
        raw_title = torrent["title"]
        
        # Check required languages filter (custom implementation since RTN doesn't filter properly)
        if required_languages:
            torrent_languages = parsed.languages if hasattr(parsed, 'languages') else []
            if not check_required_languages(torrent_languages, required_languages):
                excluded_by_language += 1
                # Log first few excluded torrents for debugging
                if excluded_by_language <= 3:
                    print(f"[LANG_FILTER] Excluded: '{raw_title}' - detected langs: {torrent_languages}, required: {required_languages}")
                continue
            else:
                included_count += 1
                if included_count <= 3:
                    print(f"[LANG_FILTER] Included: '{raw_title}' - detected langs: {torrent_languages}")

        is_fetchable, failed_keys = check_fetch(parsed, rtn_settings)
        rank = get_rank(parsed, rtn_settings, rtn_ranking)

        if remove_trash:
            if not is_fetchable or rank < rtn_settings.options["remove_ranks_under"]:
                continue

        try:
            ranked_torrents.add(
                Torrent(
                    infohash=info_hash,
                    raw_title=raw_title,
                    data=parsed,
                    fetch=is_fetchable,
                    rank=rank,
                    lev_ratio=0.0,
                )
            )
        except Exception:
            pass

    return sort_torrents(ranked_torrents, max_results_per_resolution)
