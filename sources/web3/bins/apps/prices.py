import asyncio
from sources.web3.bins.mixed.price_utilities import price_scraper


def add_prices_to_hypervisor(hypervisor: dict, network: str) -> dict:
    """Try to add usd prices for the hypervisor's tokens and LPtoken using price scraper ( coingecko + subgraph)

    Args:
        hypervisor (dict): database hypervisor item
        network (str):

    Returns:
        dict: database hypervisor item with usd prices
    """
    try:
        # get rewards data
        price_helper = price_scraper(cache=False)

        # get token prices
        price_token0 = price_helper.get_price(
            network=network,
            token_id=hypervisor["token0"]["address"],
            block=hypervisor["block"],
        )
        price_token1 = price_helper.get_price(
            network=network,
            token_id=hypervisor["token1"]["address"],
            block=hypervisor["block"],
        )

        # get LPtoken price
        price_lpToken = (
            price_token0
            * (
                int(hypervisor["totalAmounts"]["total0"])
                / (10 ** int(hypervisor["pool"]["token0"]["decimals"]))
            )
            + price_token1
            * (
                int(hypervisor["totalAmounts"]["total1"])
                / (10 ** int(hypervisor["pool"]["token1"]["decimals"]))
            )
        ) / (
            int(hypervisor["totalSupply"]["totalSupply"])
            / (10 ** hypervisor["decimals"])
        )

        hypervisor["lpToken_price_usd"] = price_lpToken
        hypervisor["token0_price_usd"] = price_token0
        hypervisor["token1_price_usd"] = price_token1

    except Exception as err:
        pass

    return hypervisor


async def get_token_price_usd(token_address: str, network: str, block: int) -> float:
    """Try to get usd price for the token using price scraper ( coingecko + subgraph)

    Args:
        token_address (str): token address
        network (str):
        block (int):

    Returns:
        float: token price in usd
    """
    price_helper = price_scraper(cache=False)

    try:
        price_token = price_helper.get_price(
            network=network,
            token_id=token_address,
            block=block,
        )
    except Exception:
        price_token = 0

    return price_token
