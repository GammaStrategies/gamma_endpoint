import time
import logging
from typing import Any
from starlette.types import ASGIApp, Scope, Receive, Send, Message
from starlette.datastructures import MutableHeaders

from endpoint.config.version import GIT_BRANCH, APP_VERSION, get_version_info


logger = logging.getLogger(__name__)


class BaseMiddleware:
    def __init__(
        self,
        app: ASGIApp,
        response_time: bool = True,
        git_branch: bool = True,
        app_version: bool = True,
    ):
        self.app = app

        self._response_time = response_time
        self._git_branch = git_branch
        self._app_version = app_version

    @property
    def response_time(self) -> bool:
        return self._response_time

    @property
    def git_branch(self) -> bool:
        return self._git_branch

    @property
    def app_version(self) -> bool:
        return self._app_version

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        # not http request
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # http request
        start_time = time.time()

        # modify send function
        def send_wrapper(message: Message):
            try:
                # modify header
                if "headers" in message:
                    headers = MutableHeaders(scope=message)
                    headers.update(
                        {
                            k: v
                            for k, v in self.build_headers(
                                start_time=start_time
                            ).items()
                        }
                    )
            except Exception as e:
                logger.error(f"Error in middleware: {e}")

            # return message
            return send(message)

        await self.app(scope, receive, send_wrapper)

    def build_headers(self, start_time: Any | None = None) -> dict:
        headers = {}
        if self.response_time and start_time:
            headers["X-responseTime"] = f"{ time.time() - start_time} sec"
        if self.git_branch:
            headers["X-branch"] = GIT_BRANCH
        if self.app_version:
            headers["X-version"] = APP_VERSION

        return headers


class DatabaseMiddleWare(BaseMiddleware):
    def __init__(
        self,
        app: ASGIApp,
    ):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        # not http request
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # http request
        # modify send function
        def send_wrapper(message: Message):
            try:
                # modify header
                if "headers" in message:
                    headers = MutableHeaders(scope=message)
                    headers.update({k: v for k, v in self.build_headers().items()})
            except Exception as e:
                logger.error(f"Error in middleware: {e}")

            # return message
            return send(message)

        await self.app(scope, receive, send_wrapper)

    def build_headers(self) -> dict:
        headers = {}
        headers["X-database"] = "true"

        return headers
