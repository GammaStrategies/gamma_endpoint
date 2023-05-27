import logging
from typing import Any
from fastapi import Response
import httpx


logger = logging.getLogger(__name__)
async_client = httpx.AsyncClient(
    transport=httpx.AsyncHTTPTransport(
        retries=1,
    ),
    timeout=60,
)


def check_all_urls(fastapi_url: str):
    # query url-list to get all urls to be tested
    for url in get_urls_to_test(fastapi_url=fastapi_url):
        logger.info(f"Test {url} -->  {test_url(url)}")


async def test_url(url: str) -> str:
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


async def get_urls_to_test(fastapi_url: str) -> list[str]:
    response = await async_client.get(url=fastapi_url)

    if response.status_code == 200:
        return response.json()
    else:
        return {}


if __name__ == "__main__":
    test_url("https://localhost:8080/")
