import json
import logging
import time
from fastapi import Request, Response

from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware import Middleware
from endpoint.config.middleware import BaseMiddleware

from endpoint.config.version import GIT_BRANCH, APP_VERSION, get_version_info

from sources.subgraph.endpoint.app import create_app as create_subgraph_endpoint
from sources.web3.endpoint.app import create_app as create_web3_endpoint
from sources.mongo.endpoint.app import create_app as create_mongo_endpoint
from sources.internal.endpoint.app import create_app as create_internal_endpoint

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
