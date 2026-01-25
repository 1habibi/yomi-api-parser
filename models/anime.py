from typing import Dict, Any, Optional, List
from datetime import datetime, date
from config import YEAR_MIN, YEAR_MAX
from utils.logging_config import logger

AnimeDict = Dict[str, Any]
MaterialDict = Dict[str, Any]


def validate_year(year: Optional[int]) -> bool:
    if year is None:
        return True
    return YEAR_MIN <= year <= YEAR_MAX


def validate_url(url: Optional[str]) -> bool:
    if url is None:
        return True
    return isinstance(url, str) and len(url) > 0


def validate_rating(rating: Optional[float]) -> bool:
    if rating is None:
        return True
    return 0.0 <= rating <= 10.0


def extract_genres(material: Optional[MaterialDict]) -> List[str]:
    if not material:
        return []

    genres = (
        material.get("anime_genres") or
        material.get("genres") or
        material.get("all_genres") or
        []
    )

    return [g for g in genres if g]


def extract_screenshots(item: AnimeDict, material: Optional[MaterialDict]) -> List[str]:
    screenshots = []

    if item.get("screenshots"):
        screenshots.extend(item["screenshots"])

    if material and material.get("screenshots"):
        screenshots.extend(material["screenshots"])

    # Return unique URLs while preserving order
    return list(dict.fromkeys(screenshots))


def get_person_mapping(material: Optional[MaterialDict]) -> Dict[str, List[str]]:
    if not material:
        return {}

    return {
        "actor": material.get("actors") or [],
        "director": material.get("directors") or [],
        "producer": material.get("producers") or [],
        "writer": material.get("writers") or [],
        "composer": material.get("composers") or [],
    }


def get_studios(material: Optional[MaterialDict]) -> List[str]:
    if not material:
        return []

    studios = material.get("anime_studios") or []
    return [s for s in studios if s]


def get_blocked_countries(item: AnimeDict) -> List[str]:
    return item.get("blocked_countries") or []


def normalize_blocked_seasons(blocked_seasons: Any) -> Optional[Dict[str, Any]]:
    if not blocked_seasons:
        return None

    if isinstance(blocked_seasons, dict):
        return blocked_seasons

    if isinstance(blocked_seasons, str) and blocked_seasons == "all":
        return {"all": "all"}

    logger.warning(f"Unexpected blocked_seasons format: {type(blocked_seasons)}")
    return None
