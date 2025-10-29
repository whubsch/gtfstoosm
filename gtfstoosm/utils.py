import random
import re

import atlus
from pydantic import BaseModel


class Trip(BaseModel):
    trip_id: str | int
    route_id: str | int
    shape_id: str | int
    stops: list[str | int]


def string_to_unique_int(text: str, max_int: int = 2**31 - 1) -> int:
    """
    Convert any string to a unique positive integer.

    Args:
        text: The input string
        max_int: Maximum integer value (default is max 32-bit signed integer)
    """
    if not text:
        text = str(random.randint(0, max_int))
    # Create a positive integer hash
    hash_value = hash(text) & 0x7FFFFFFF  # Mask to ensure positive value
    return hash_value % max_int  # Ensure it's within range


def deduplicate_trips(trips: list[Trip]) -> list[Trip]:
    """
    Remove duplicate trips based on their stops attribute.

    Args:
        trips: List of Trip objects to deduplicate

    Returns:
        List of unique Trip objects (first occurrence kept for each unique stops sequence)
    """
    seen_stops = set()
    unique_trips = []

    for trip in trips:
        # Convert stops list to tuple for hashing
        stops_tuple = tuple(trip.stops)
        if stops_tuple not in seen_stops:
            seen_stops.add(stops_tuple)
            unique_trips.append(trip)

    return unique_trips


def calculate_direction(
    start_coordinate: tuple[float, float], end_coordinate: tuple[float, float]
) -> str:
    """
    Calculate the direction of a route based on its start and end coordinates.

    Args:
        start_coordinate: Tuple of start latitude and longitude
        end_coordinate: Tuple of end latitude and longitude

    Returns:
        String representing the direction of the route
    """
    # Convert coordinates to floats
    start_latitude, start_longitude = (
        float(start_coordinate[0]),
        float(start_coordinate[1]),
    )
    end_latitude, end_longitude = float(end_coordinate[0]), float(end_coordinate[1])

    lat_diff = end_latitude - start_latitude
    lon_diff = end_longitude - start_longitude

    # Compare absolute differences to determine primary direction
    if abs(lat_diff) > abs(lon_diff):
        # Movement is primarily north-south
        if lat_diff > 0:
            return "Northbound"
        else:
            return "Southbound"
    else:
        # Movement is primarily east-west
        if lon_diff > 0:
            return "Eastbound"
        else:
            return "Westbound"


# Use a capturing group to keep the separators in the result
split_compile = re.compile(r"([/\-–—|\\~])")


def format_name(name: str) -> str:
    """
    Format a name for use in OSM.

    Args:
        name: The input name
    """
    # Remove leading and trailing whitespace
    name = name.strip(" ,;").replace("  ", " ").replace("_", " ")

    # Split the text while capturing the separators
    parts = split_compile.split(name)

    processed_parts = []

    for i, part in enumerate(parts):
        # If this is a separator (which will be at odd indices after the split)
        if i % 2 == 1:
            # This is a separator, keep it as is
            processed_parts.append(part)
        elif part.strip():  # Non-empty text part
            # Process through atlus.abbrs and clean up whitespace
            part = part.strip()
            abbreviated = atlus.abbrs(
                atlus.get_title(part, single_word=bool(" " not in part))
            )
            processed_parts.append(abbreviated)

    return (
        "".join(processed_parts)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def parse_tag_string(tag_string: str) -> dict[str, str]:
    """
    Parse a semicolon-separated key=value string into a dictionary.

    Args:
        tag_string: String in format "key1=value1;key2=value2"

    Returns:
        Dictionary of key-value pairs
    """
    result = {}
    for pair in tag_string.split(";"):
        pair = pair.strip()
        if "=" not in pair:
            continue
        key, value = pair.split("=", 1)
        result[key.strip()] = value.strip()
    return result
