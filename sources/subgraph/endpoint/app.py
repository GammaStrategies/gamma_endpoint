import logging

from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend

from sources.subgraph.endpoint.routers import build_routers, build_routers_compatible
from sources.subgraph.bins.config import gamma_clients, DEPLOYMENTS, RUN_MODE
from sources.subgraph.bins.subgraphs.gamma import GammaClient

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    """Define actions for lifespan of app"""
    if RUN_MODE != "DEV":
        for i, (protocol, chain) in enumerate(DEPLOYMENTS):
            logger.info(
                "(%s/%s) Init GammaClient for %s-%s",
                i + 1,
                len(DEPLOYMENTS),
                protocol.value,
                chain.value,
            )
            gamma_clients[protocol][chain] = GammaClient(protocol, chain)
    logger.info("Initiating FastAPI cache")
    FastAPICache.init(InMemoryBackend(), prefix="fastapi-cache")
    yield


def create_app(
    title: str,
    backwards_compatible: bool = False,
    version="0.1",
):
    """Create app for Subgraph"""
    app = FastAPI(
        title=title,
        lifespan=lifespan,
        swagger_ui_parameters={"docExpansion": "none"},
        version=version,
    )

    # Add subgraph routes to app
    for route_builder in (
        build_routers() if not backwards_compatible else build_routers_compatible()
    ):
        app.include_router(route_builder.router(), tags=route_builder.tags)

    # Allow CORS
    app.add_middleware(
        CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
    )

    return app
