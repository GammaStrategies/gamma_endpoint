def filter_addresses(addresses: list[str]) -> list[str] | None:
    """Discard any non address str from a list of addresses.

    Args:
        addresses (list[str]): list of addresses

    Returns:
        list[str] | None: list of addresses or None
    """
    # remove any non address str
    if addresses:
        return [
            h.lower()
            for h in addresses
            if h and h.startswith("0x") and len(h) == 42 and h[2:].isalnum()
        ]
    return None
