from fastapi import FastAPI
from fastapi import Request
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend

from fastapi.middleware.cors import CORSMiddleware
from endpoint.config.cache import CHARTS_CACHE_TIMEOUT
from endpoint.config.middleware import DatabaseMiddleWare

from sources.strats.endpoint.routers import build_routers


from fastapi.exceptions import RequestValidationError
from fastapi.responses import PlainTextResponse


def create_app(
    title: str,
    version="0.0.1",
):
    description = """

                Gamma Strats API
            
                Mainly focused in sourcing specialized data for Gamma strats.
                
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
        # swagger_ui_parameters={"docExpansion": "none"},
        version=version,
    )

    # Add subgraph routes to app
    for route_builder in build_routers():
        app.include_router(route_builder.router(), tags=route_builder.tags)

    # Allow CORS
    app.add_middleware(
        CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
    )

    # add database middleware
    app.add_middleware(DatabaseMiddleWare)

    @app.on_event("startup")
    async def startup():
        FastAPICache.init(InMemoryBackend())

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request, exc):
        po = ""
        return PlainTextResponse(str(exc), status_code=400)

    return app
