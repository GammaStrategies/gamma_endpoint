import sys
import datetime as dt
import requests
import logging
import time
import threading

from requests import exceptions as req_exceptions


#
def post_request(
    url: str,
    query: str,
    retry: int = 0,
    max_retry: int = 2,
    wait_secs: int = 5,
    timeout_secs: int = 10,
) -> dict:
    try:
        request = requests.post(url=url, json={"query": query}, timeout=timeout_secs)
        return request.json()
    except (req_exceptions.ConnectionError, ConnectionError) as err:
        # blocking us?  wait and try as many times as defined
        logging.getLogger(__name__).warning(f"Connection to {url} has been closed...")
    except req_exceptions.ReadTimeout as err:
        logging.getLogger(__name__).warning(f"Connection to {url} has timed out...")
    except Exception:
        logging.getLogger(__name__).exception(
            f"Unexpected error while posting request at {url} .error: {sys.exc_info()[0]}"
        )

    # check if retry is needed
    if retry < max_retry:
        logging.getLogger(__name__).warning(
            f"    Waiting {wait_secs} seconds to retry {url} query for the {retry} time."
        )

        time.sleep(wait_secs)
        # retry
        return post_request(
            url=url,
            query=query,
            retry=retry + 1,
            max_retry=max_retry,
            wait_secs=wait_secs,
            timeout_secs=timeout_secs,
        )

    # return empty dict
    return {}


def get_request(
    url,
    retry: int = 0,
    max_retry: int = 2,
    wait_secs: int = 5,
    timeout_secs: int = 10,
) -> dict:
    result = {}
    # query url
    try:
        result = requests.get(url=url, timeout=timeout_secs).json()
        return result

    except (req_exceptions.ConnectionError, ConnectionError) as err:
        # thegraph blocking us?
        # wait and try one last time
        logging.getLogger(__name__).warning(f"Connection error to {url}...")
    except Exception:
        logging.getLogger(__name__).exception(
            f"Unexpected error while retrieving json from {url}     .error: {sys.exc_info()[0]}"
        )

    if retry < max_retry:
        logging.getLogger(__name__).debug(
            f"    Waiting {wait_secs} seconds to retry {url} query for the {retry} time."
        )

        time.sleep(wait_secs)
        return get_request(
            url=url,
            retry=retry + 1,
            max_retry=max_retry,
            wait_secs=wait_secs,
            timeout_secs=timeout_secs,
        )


class rate_limit:
    def __init__(self, rate_max_sec: float):
        self.rate_max_sec: float = rate_max_sec
        self.rate_sec: int = 0
        self.rate_count_lastupdate: dt.datetime = dt.datetime.now() - dt.timedelta(
            hours=8
        )
        self.lock = threading.Lock()  # threading.RLock

    def hit(self) -> bool:
        """Report a query to rate limit and
           return if I'm safe proceeding

        Returns:
           [bool] -- Im I safe ??
        """
        with self.lock:
            # update qtty rate per second so sum only if 1 sec has not yet passed
            if (dt.datetime.now() - self.rate_count_lastupdate).total_seconds() <= 1:
                # set current rate
                self.rate_sec += 1
            else:
                self.rate_sec = 1
                # update last date
                self.rate_count_lastupdate = dt.datetime.now()

        # return if i'm save to go
        return self.im_safe()

    def im_safe(self) -> bool:
        """Is it safe to continue

        Returns:
           bool -- [description]
        """
        return self.rate_sec <= self.rate_max_sec

    def continue_when_safe(self):
        """Wait here till rate is in bounds"""

        while (self.im_safe) == False:
            with self.lock:
                if (
                    dt.datetime.now() - self.rate_count_lastupdate
                ).total_seconds() <= 1:
                    # set current rate
                    self.rate_sec += 1
                else:
                    self.rate_sec = 1
                    # update last date
                    self.rate_count_lastupdate = dt.datetime.now()

        # keep track
        self.hit()
