from sources.common.general.enums import Chain, Protocol

# TODO: save this in the database configuration
# Gamma fee Revenue is not always the exact amounts transfered to the feeRecipients.
# Sometimes, special agreements are made where Gamma collects 100% of a 'gross revenue' value but gets only a percentage of that.
# Here we define the fee in a by chain dex basis.
# chain:{ dex(protocol database name): fee multiplier }
REVENUE_FEE_OVERWRITE = {
    Chain.ARBITRUM: {
        Protocol.CAMELOT.database_name: 0.623529,
    },
    Chain.POLYGON: {Protocol.QUICKSWAP.database_name: 0.5},
    Chain.POLYGON_ZKEVM: {
        Protocol.QUICKSWAP.database_name: 0.5,
        Protocol.QUICKSWAP_UNISWAP.database_name: 0.5,
    },
    Chain.MANTA: {Protocol.QUICKSWAP.database_name: 0.5},
    Chain.BASE: {Protocol.THICK.database_name: 0.2, Protocol.BASEX.database_name: 0.2},
    Chain.LINEA: {Protocol.LYNEX.database_name: 0.2},
    Chain.ASTAR_ZKEVM: {Protocol.QUICKSWAP.database_name: 0.5},
    Chain.IMMUTABLE_ZKEVM: {Protocol.QUICKSWAP.database_name: 0.5},
    Chain.BLAST: {Protocol.BLASTER.database_name: 0.2},
    Chain.BSC: {
        # approx 0.07 + spNFT ( not accurate )
        Protocol.THENA.database_name: 0.068
    },
}


def addFields_usdvalue_revenue_query_part(chain: Chain) -> dict:
    """Add a usd_value field to the query, with fee overrides, if any

    Args:
        chain (Chain): chain

    Returns:
        dict: "$addFields" query part
    """

    # check if there are fee overrides to apply
    branches = []
    for dex, fee_multiplier in REVENUE_FEE_OVERWRITE.get(chain, {}).items():
        branches.append(
            {
                "case": {"$eq": ["$dex", dex]},
                "then": {"$multiply": ["$usd_value", fee_multiplier]},
            }
        )

    if branches:
        return {
            "$addFields": {
                "usd_value": {
                    "$switch": {
                        "branches": branches,
                        "default": "$usd_value",
                    }
                }
            }
        }
    else:
        return {"$addFields": {"usd_value": "$usd_value"}}
