from pydantic import BaseModel


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
