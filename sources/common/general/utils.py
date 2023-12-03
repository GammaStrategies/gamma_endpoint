def filter_addresses(addresses: list[str] | str) -> list[str] | str | None:
    """Discard any non address str from addresses.

    Args:
        addresses (list[str] or str): list of addresses or one address

    Returns:
        list[str] | str | None: addresses or None
    """
    # remove any non address str
    if addresses:
        if isinstance(addresses, str):
            return (
                addresses.lower()
                if addresses
                and addresses.startswith("0x")
                and len(addresses) == 42
                and addresses[2:].isalnum()
                else None
            )
        else:
            return [
                h.lower()
                for h in addresses
                if h and h.startswith("0x") and len(h) == 42 and h[2:].isalnum()
            ]
    return None
