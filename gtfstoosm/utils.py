from pydantic import BaseModel
import re
import atlus


class Trip(BaseModel):
    trip_id: int
    route_id: str | int
    shape_id: str | int
    stops: list[int]


def string_to_unique_int(text: str, max_int: int = 2**31 - 1) -> int:
    """
    Convert any string to a unique positive integer.

    Args:
        text: The input string
        max_int: Maximum integer value (default is max 32-bit signed integer)
    """
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


# Use a capturing group to keep the separators in the result
split_compile = re.compile(r"([/\-–—|\\~])")


def format_name(name: str) -> str:
    """
    Format a name for use in OSM.

    Args:
        name: The input name
    """
    # Remove leading and trailing whitespace
    name = name.strip(" ,;").replace("  ", " ")

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
