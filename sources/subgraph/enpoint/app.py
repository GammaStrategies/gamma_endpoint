from fastapi import FastAPI
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend

from fastapi.middleware.cors import CORSMiddleware
from endpoint.config.cache import CHARTS_CACHE_TIMEOUT

from sources.subgraph.enpoint.routers import build_routers, build_routers_compatible


def create_app(
    title: str,
    backwards_compatible: bool = False,
    version="0.5",
):
    app = FastAPI(
        title=title,
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

    @app.on_event("startup")
    async def startup():
        FastAPICache.init(InMemoryBackend())

    return app
