from fastapi import FastAPI
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend

from fastapi.middleware.cors import CORSMiddleware
from endpoint.config.cache import CHARTS_CACHE_TIMEOUT

from sources.web3.endpoint.routers import build_routers


def create_app(
    title: str,
    version="0.0.1",
):
    description = """

                Gamma Web3 API
            
                Mainly focused in sourcing data from direct onchain calls.
                Some fields, like prices, may be gathered from any available source.

    """
    contact = (
        {
            "name": "Gamma Strategies",
            "url": "https://www.gamma.xyz/",
        },
    )

    app = FastAPI(
        title=title,
        description=description,
        swagger_ui_parameters={"docExpansion": "none"},
        version=version,
    )

    # Add subgraph routes to app
    for route_builder in build_routers():
        app.include_router(route_builder.router(), tags=route_builder.tags)

    # Allow CORS
    app.add_middleware(
        CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
    )

    @app.on_event("startup")
    async def startup():
        FastAPICache.init(InMemoryBackend())

    return app
