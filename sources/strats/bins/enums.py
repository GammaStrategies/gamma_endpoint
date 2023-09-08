from sources.common.general.enums import Chain, text_to_chain


def convert_chain_name(chain: Chain | str) -> str:
    """convert chain name to suitable name for strats

    Args:
        chain (Chain | None, optional): . Defaults to None.
        chain_name (str | None, optional): . Defaults to None.

    Raises:
        ValueError:

    Returns:
        str: chain
    """
    if isinstance(chain, Chain):
        if chain == Chain.POLYGON_ZKEVM:
            return "zkevm"
        else:
            return chain.subgraph_name
    elif isinstance(chain, str):
        if text_to_chain(chain) == Chain.POLYGON_ZKEVM:
            return "zkevm"
        else:
            chain = text_to_chain(chain)
            return chain.subgraph_name
    else:
        raise ValueError("Chain must be provided")
