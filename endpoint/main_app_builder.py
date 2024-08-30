import logging
import sys

from fastapi.middleware.cors import CORSMiddleware
from fastapi_cache import FastAPICache
from endpoint.config.middleware import BaseMiddleware

from endpoint.config.version import get_version_info

from sources.subgraph.endpoint.app import create_app as create_subgraph_endpoint
from sources.web3.endpoint.app import create_app as create_web3_endpoint
from sources.mongo.endpoint.app import create_app as create_mongo_endpoint
from sources.internal.endpoint.app import create_app as create_internal_endpoint
from sources.strats.endpoint.app import create_app as create_strats_endpoint
from sources.frontend.endpoint.app import create_app as create_frontend_endpoint

# Overwrite the return of Decimal type as string ( instead of float )
# from fastapi import encoders
# encoders.ENCODERS_BY_TYPE[decimal.Decimal] = str

logging.basicConfig(
    format="[%(asctime)s:%(levelname)s:%(name)s]:%(message)s",
    datefmt="%Y/%m/%d %I:%M:%S",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)

# Create root
app = create_subgraph_endpoint(
    title="Gamma API", backwards_compatible=True, version=get_version_info()
)  # legacy endpoint

# Allow CORS
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

app.add_middleware(BaseMiddleware)


# Create subgraph endpoint ------------------------
app.mount(
    path="/subgraph",
    app=create_subgraph_endpoint(
        title="Gamma API - Subgraph data",
        backwards_compatible=False,
        version=get_version_info(),
    ),
    name="subgraph",
)

# Create mongodb endpoint ------------------------
app.mount(
    path="/database",
    app=create_mongo_endpoint(
        title="Gamma API - web3 database", version=get_version_info()
    ),
    name="database",
)

# Create web3 endpoint ------------------------
app.mount(
    path="/web3",
    app=create_web3_endpoint(
        title="Gamma API - web3 calls", version=get_version_info()
    ),
    name="web3",
)

# Create internal endpoint ------------------------
app.mount(
    path="/internal",
    app=create_internal_endpoint(
        title="Gamma API - Internal", version=get_version_info()
    ),
    name="internal",
)

# Create strats endpoint ------------------------
app.mount(
    path="/strats",
    app=create_strats_endpoint(title="Gamma API - Strats", version=get_version_info()),
    name="strats",
)

# Create frontend endpoint ------------------------
app.mount(
    path="/frontend",
    app=create_frontend_endpoint(
        title="Gamma API - Frontend", version=get_version_info()
    ),
    name="frontend",
)


if hasattr(sys, "gettrace") and sys.gettrace() is not None:
    # to be able to auto test all endpoint urls
    @app.get("/url-list", include_in_schema=False)
    def get_all_urls():
        url_list = []

        def add_urls(original_list: list, routes, parent_path: str | None = None):
            for route in routes:
                if subroutes := getattr(route, "routes", None):
                    if parent_path:
                        add_urls(
                            original_list, route.routes, f"{parent_path}{route.path}"
                        )
                    else:
                        add_urls(original_list, route.routes, route.path)

                if parent_path:
                    original_list.append(
                        {"path": f"{parent_path}{route.path}", "name": route.name}
                    )
                else:
                    original_list.append({"path": route.path, "name": route.name})

        add_urls(url_list, app.routes)
        return url_list

    @app.get("/clear-cache", include_in_schema=False)
    async def clear_cache():
        await FastAPICache.clear()
