import logging

from fastapi.middleware.cors import CORSMiddleware

from sources.subgraph.enpoint.app import create_app as create_subgraph_endpoint
from sources.web3.endpoint.app import create_app as create_web3_endpoint
from sources.mongo.endpoint.app import create_app as create_mongo_endpoint

logging.basicConfig(
    format="[%(asctime)s:%(levelname)s:%(name)s]:%(message)s",
    datefmt="%Y/%m/%d %I:%M:%S",
    level=logging.INFO,
)

# Create root
app = create_subgraph_endpoint(
    title="Gamma API", backwards_compatible=True, version="1"
)  # legacy endpoint

# Allow CORS
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

# Create subgraph endpoint
app.mount(
    path="/subgraph",
    app=create_subgraph_endpoint(
        title="Gamma API", backwards_compatible=False, version="0.0.1"
    ),
    name="subgraph",
)

# Create mongodb endpoint
app.mount(
    path="/database",
    app=create_mongo_endpoint(title="Gamma API - web3 database", version="0.0.1"),
    name="database",
)

# Create web3 endpoint
app.mount(
    path="/web3",
    app=create_web3_endpoint(title="Gamma API - web3 calls", version="0.0.1"),
    name="web3",
)
