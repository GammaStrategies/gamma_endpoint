import asyncio
import logging
from typing import Any
from fastapi import Response
import httpx

logging.basicConfig(
    format="[%(asctime)s:%(levelname)s:%(name)s]:%(message)s",
    datefmt="%Y/%m/%d %I:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)
async_client = httpx.AsyncClient(
    transport=httpx.AsyncHTTPTransport(
        retries=1,
    ),
    timeout=60,
)


async def check_all_urls(fastapi_url: str):
    # query url-list to get all urls to be tested
    for url_object in await get_urls_to_test(fastapi_url=fastapi_url):
        url = f"{fastapi_url}{url_object['path']}"
        logger.info(f"Test {url} -->  {await test_url_status_code(url)}")


async def test_url_status_code(url: str) -> str:
    """checks response status code only

    Args:
        url (str):

    Returns:
        str: pass or fail
    """
    result = False
    try:
        response = await async_client.get(url)

        if response.status_code == 200:
            result = True
        else:
            logger.error(
                " Unexpected response code {} received testing {} resp.text: {} ".format(
                    response.status_code,
                    url,
                    response.text,
                )
            )
            result = False
    except Exception as e:
        logger.error(f" Unexpected error testing {url}-> {e}")

    return "pass" if result else "fail"


async def get_urls_to_test(
    fastapi_url: str, path_to_list: str = "/url-list"
) -> list[dict]:
    """Get a list of urls to be tested

    Args:
        fastapi_url (str):
        path_to_list (str, optional): . Defaults to "/url-list".

    Returns:
        list[dict]: "path": <url>  "name":<name>
    """
    response = await async_client.get(url=f"{fastapi_url}{path_to_list}")

    if response.status_code == 200:
        return response.json()
    else:
        return {}


if __name__ == "__main__":
    asyncio.run(check_all_urls("https://wire3.gamma.xyz"))
