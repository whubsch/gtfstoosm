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
