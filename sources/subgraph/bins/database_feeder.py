#
#   Script to update mongoDb with periodic data
#
import asyncio
import getopt
import logging
import os
import sys
from datetime import datetime, timezone

from aiocron import crontab
from croniter import croniter

########################################
# append parent directory pth
CURRENT_FOLDER = os.path.dirname(os.path.realpath(__file__))
PARENT_FOLDER = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_FOLDER)))
sys.path.append(PARENT_FOLDER)
########################################

from sources.common.general.enums import Chain, Period
from sources.subgraph.bins import utils
from sources.subgraph.bins.config import (
    EXCLUDED_HYPERVISORS,
    GAMMA_SUBGRAPH_URLS,
    MONGO_DB_URL,
)
from sources.subgraph.bins.database.managers import (
    db_aggregateStats_manager,
    db_allData_manager,
    db_allRewards2_external_manager,
    db_allRewards2_manager,
    db_returns_manager,
    db_static_manager,
)
from sources.subgraph.bins.enums import Protocol

logging.basicConfig(
    format="[%(asctime)s:%(levelname)s:%(name)s]:%(message)s",
    datefmt="%Y/%m/%d %I:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# using gamma subgraph keys to build chain,protocol list
CHAINS_PROTOCOLS = [
    (chain, protocol)
    for protocol in Protocol
    if protocol in GAMMA_SUBGRAPH_URLS
    for chain in GAMMA_SUBGRAPH_URLS[protocol].keys()
]

# Rewards managed by gamma utilities ( 3rd party rewards not in subgraph)
# PROTOCOL_REWARDS_DATABASE = [
#     Protocol.ZYBERSWAP,
#     Protocol.THENA,
#     Protocol.SUSHI,
#     Protocol.BEAMSWAP,
#     Protocol.RAMSES,
#     Protocol.SYNTHSWAP,
# ]
PROTOCOL_CHAIN_REWARDS_DATABASE = {
    Protocol.UNISWAP: [Chain.ARBITRUM],
    Protocol.QUICKSWAP: [],
    Protocol.ZYBERSWAP: [Chain.ARBITRUM],
    Protocol.THENA: [Chain.BSC],
    Protocol.CAMELOT: [Chain.ARBITRUM],
    Protocol.GLACIER: [],
    Protocol.RETRO: [],
    Protocol.STELLASWAP: [],
    Protocol.BEAMSWAP: [Chain.MOONBEAM],
    Protocol.SPIRITSWAP: [],
    Protocol.SUSHI: [Chain.POLYGON, Chain.ARBITRUM, Chain.BASE],
    Protocol.RAMSES: [Chain.ARBITRUM],
    Protocol.ASCENT: [],
    Protocol.FUSIONX: [],
    Protocol.SYNTHSWAP: [Chain.BASE],
    Protocol.LYNEX: [],
    Protocol.PEGASYS: [],
}

# set cron vars
EXPR_FORMATS = {
    "returns": {
        Period.DAILY.value: Period.DAILY.cron,  # (At every 60th minute past every 2nd hour. )
        Period.WEEKLY.value: Period.WEEKLY.cron,  # (At every 60th minute past every 12th hour. )  # can't do every 14 hours
        Period.BIWEEKLY.value: Period.BIWEEKLY.cron,  # ( At 06:00 on every day-of-month.)
        Period.MONTHLY.value: Period.MONTHLY.cron,  # ( At 12:00 on every 2nd day-of-month.)
        Period.QUARTERLY.value: Period.QUARTERLY.cron,  # ( At 00:00 on every 6rd day-of-month.)
        Period.BIANNUAL.value: Period.BIANNUAL.cron,  # ( At 00:00 on every 12th day-of-month.)
        Period.YEARLY.value: Period.YEARLY.cron,  # ( At 00:00 on every 24th day-of-month.)
    },
    "inSecuence": {  # allData + static hypervisor info
        "mins": "*/30 * * * *",
    },
    "aggregateStats": {
        "mins": "*/15 * * * *",
    },
    "allRewards2": {
        "mins": "*/20 * * * *",
    },
}
EXPR_ARGS = {
    "returns": {
        Period.DAILY.value: [[Period.DAILY.days]],
        Period.WEEKLY.value: [[Period.WEEKLY.days]],
        Period.BIWEEKLY.value: [[Period.BIWEEKLY.days]],
        Period.MONTHLY.value: [[Period.MONTHLY.days]],
        Period.QUARTERLY.value: [[Period.QUARTERLY.days]],
        Period.BIANNUAL.value: [[Period.BIANNUAL.days]],
        Period.YEARLY.value: [[Period.YEARLY.days]],
    }
}


# feed jobs
async def feed_database_returns(
    periods: list, current_timestamp: int = None, max_retries: int = 1
):
    name = "returns"
    logger.info(f" Starting database feeding process for {name} data")
    # start time log
    _startime = datetime.now(timezone.utc)

    returns_manager = db_returns_manager(mongo_url=MONGO_DB_URL)
    returns_manager._max_retry = max_retries

    # all request at once
    requests = [
        returns_manager.feed_db(
            chain=chain,
            protocol=protocol,
            periods=periods,
            current_timestamp=current_timestamp,
        )
        for chain, protocol in CHAINS_PROTOCOLS
    ]
    await asyncio.gather(*requests)

    # end time log
    logger.info(f" took {get_timepassed_string(_startime)} to complete the {name} feed")


async def feed_database_static():
    name = "static"
    logger.info(f" Starting database feeding process for {name} data")
    logger.debug(f"     chains prot.: {CHAINS_PROTOCOLS}")
    logger.debug(f"     excluded_hyp: {EXCLUDED_HYPERVISORS}")

    # start time log
    _startime = datetime.now(timezone.utc)

    # static requests
    static_manager = db_static_manager(mongo_url=MONGO_DB_URL)
    requests = [
        static_manager.feed_db(chain=chain, protocol=protocol)
        for chain, protocol in CHAINS_PROTOCOLS
    ]

    # execute feed
    await asyncio.gather(*requests)

    # end time log
    logger.info(f" took {get_timepassed_string(_startime)} to complete the {name} feed")


async def feed_database_allData():
    name = "allData"
    logger.info(f" Starting database feeding process for {name} data")
    logger.debug(f"     chains prot.: {CHAINS_PROTOCOLS}")
    logger.debug(f"     excluded_hyp: {EXCLUDED_HYPERVISORS}")

    # start time log
    _startime = datetime.now(timezone.utc)

    _manager = db_allData_manager(mongo_url=MONGO_DB_URL)
    requests = [
        _manager.feed_db(
            chain=chain,
            protocol=protocol,
        )
        for chain, protocol in CHAINS_PROTOCOLS
    ]

    # execute feed
    await asyncio.gather(*requests)

    # end time log
    logger.info(f" took {get_timepassed_string(_startime)} to complete the {name} feed")


async def feed_all_allRewards2():
    await feed_database_allRewards2()
    await feed_database_allRewards2_externals()


async def feed_database_allRewards2():
    name = "allRewards2"
    logger.info(f" Starting database feeding process for {name} data")
    # start time log
    _startime = datetime.now(timezone.utc)

    _manager = db_allRewards2_manager(mongo_url=MONGO_DB_URL)
    requests = [
        _manager.feed_db(
            chain=chain,
            protocol=protocol,
        )
        for chain, protocol in CHAINS_PROTOCOLS
        # if chain not in PROTOCOL_CHAIN_REWARDS_DATABASE.get(protocol, [])
    ]

    # execute feed
    await asyncio.gather(*requests)

    # end time log
    logger.info(f" took {get_timepassed_string(_startime)} to complete the {name} feed")


async def feed_database_allRewards2_externals(current_timestamp: int | None = None):
    name = "allRewards2 external"
    logger.info(f" Starting database feeding process for {name} data")
    # start time log
    _startime = datetime.now(timezone.utc)

    _manager = db_allRewards2_external_manager(mongo_url=MONGO_DB_URL)
    requests = [
        _manager.feed_db(
            chain=chain,
            protocol=protocol,
            current_timestamp=current_timestamp,
        )
        for chain, protocol in CHAINS_PROTOCOLS
        if chain in PROTOCOL_CHAIN_REWARDS_DATABASE.get(protocol, [])
    ]

    # execute feed
    await asyncio.gather(*requests)

    # end time log
    logger.info(f" took {get_timepassed_string(_startime)} to complete the {name} feed")


async def feed_database_aggregateStats():
    name = "aggregateStats"
    logger.info(f" Starting database feeding process for {name} data")
    # start time log
    _startime = datetime.now(timezone.utc)

    _manager = db_aggregateStats_manager(mongo_url=MONGO_DB_URL)
    requests = [
        _manager.feed_db(
            chain=chain,
            protocol=protocol,
        )
        for chain, protocol in CHAINS_PROTOCOLS
    ]

    # execute feed
    await asyncio.gather(*requests)

    # end time log
    logger.info(f" took {get_timepassed_string(_startime)} to complete the {name} feed")


# Multiple feeds in one
async def feed_database_inSecuence():
    # start time log
    _startime = datetime.now(timezone.utc)

    await feed_database_static()
    await feed_database_allData()

    _endtime = datetime.now(timezone.utc)
    if (_endtime - _startime).total_seconds() > (60 * 2):
        # end time log
        logger.warning(
            f" Consider increasing cron schedule ->  took {get_timepassed_string(_startime, _endtime)} to complete database feeder loop."
        )


async def feed_all():
    await feed_database_static()
    await feed_database_allData()
    await feed_all_allRewards2()
    await feed_database_aggregateStats()


# Manual script execution
async def feed_database_with_historic_data(from_datetime: datetime, periods=None):
    """Fill database with historic

    Args:
        from_datetime (datetime): like datetime(2022, 12, 1, 0, 0, tzinfo=timezone.utc)
        process_quickswap (bool): should quickswap protocol be included ?
        periods (list): list of periods as ["daily", "weekly", "monthly"]
    """
    # final log var
    processed_datetime_strings = []

    last_time = datetime.now(timezone.utc)

    # define periods when empty
    if not periods:
        periods = list(EXPR_ARGS["returns"].keys())

    logger.info(
        f" Feeding database with historic data  periods:{periods} chains/protocols:{CHAINS_PROTOCOLS}"
    )

    for period in periods:
        cron_ex_format = EXPR_FORMATS["returns"][period]

        # create croniter
        c_iter = croniter(expr_format=cron_ex_format, start_time=from_datetime)
        current_timestamp = c_iter.get_next(start_time=from_datetime.timestamp())

        # set utils now
        last_timestamp = last_time.timestamp()
        while last_timestamp > current_timestamp:
            txt_timestamp = "{:%Y-%m-%d  %H:%M:%S}".format(
                datetime.utcfromtimestamp(current_timestamp)
            )
            processed_datetime_strings.append(txt_timestamp)
            logger.info(f" Feeding {period} database at  {txt_timestamp}")

            # database feed
            await asyncio.gather(
                feed_database_returns(
                    periods=EXPR_ARGS["returns"][period][0],
                    current_timestamp=int(current_timestamp),
                    max_retries=0,
                ),
                feed_database_allRewards2_externals(
                    current_timestamp=int(current_timestamp)
                ),
                return_exceptions=True,
            )

            # set next timestamp
            current_timestamp = c_iter.get_next(start_time=current_timestamp)

    logger.info(f" Processed dates: {processed_datetime_strings} ")


def convert_commandline_arguments(argv) -> dict:
    """converts command line arguments to a dictionary of those

    Arguments:
       argv {} --

    Returns:
       dict --
    """

    # GET COMMAND LINE ARGUMENTS
    prmtrs = {"historic": False}
    try:
        opts, args = getopt.getopt(argv, "hs:m:", ["historic", "start=", "manual="])
    except getopt.GetoptError as err:
        print("             <filename>.py <options>")
        print("Options:")
        print(" -s <start date> or --start=<start date>")
        print(" -m <option> or --manual=<option>")
        print("           <option> being: secuence")
        print(" ")
        print(" ")
        print(" ")
        print("to feed database with current data  (infinite loop):")
        print("             <filename>.py")
        print("to feed database with historic data: (no quickswap)")
        print("             <filename>.py -h")
        print("             <filename>.py -s <start date as %Y-%m-%d>")
        print("error message: {}".format(err.msg))
        print("opt message: {}".format(err.opt))
        sys.exit(2)

    # loop and retrieve each command
    for opt, arg in opts:
        if opt in ("-s", "start="):
            # todo: check if it is a date
            prmtrs["from_datetime"] = datetime.strptime((arg).strip(), "%Y-%m-%d")
            prmtrs["historic"] = True
        elif opt in ("-h", "historic"):
            prmtrs["historic"] = True
        elif opt in ("-m", "manual="):
            prmtrs["manual"] = arg
    return prmtrs


def get_timepassed_string(start_time: datetime, end_time: datetime = None) -> str:
    if not end_time:
        end_time = datetime.now(timezone.utc)
    _timelapse = end_time - start_time
    _passed = _timelapse.total_seconds()
    if _passed < 60:
        _timelapse_unit = "seconds"
    elif _passed < 60 * 60:
        _timelapse_unit = "minutes"
        _passed /= 60
    elif _passed < 60 * 60 * 24:
        _timelapse_unit = "hours"
        _passed /= 60 * 60
    return "{:,.2f} {}".format(_passed, _timelapse_unit)


# set functions here
EXPR_FUNCS = {
    "returns": feed_database_returns,
    "static": feed_database_static,
    "allData": feed_database_allData,
    "allRewards2": feed_all_allRewards2,
    "aggregateStats": feed_database_aggregateStats,
    "inSecuence": feed_database_inSecuence,
}

if __name__ == "__main__":
    os.chdir(PARENT_FOLDER)

    # convert command line arguments to dict variables
    cml_parameters = convert_commandline_arguments(sys.argv[1:])

    if cml_parameters["historic"]:
        # historic feed

        from_datetime = cml_parameters.get(
            "from_datetime", datetime(2022, 12, 1, 0, 0, tzinfo=timezone.utc)
        )

        logger.info(" ")
        logger.info(
            " Feeding database with historic data from {:%Y-%m-%d} to now *********************   ********************* ".format(
                from_datetime
            )
        )
        logger.info(" ")

        # start time log
        _startime = datetime.now(timezone.utc)

        asyncio.run(feed_database_with_historic_data(from_datetime=from_datetime))

        # end time log
        logger.info(
            f" took {get_timepassed_string(_startime)} to complete the historic feed"
        )

    elif "manual" in cml_parameters:
        logger.info(" Starting one-time manual execution ")
        logger.info(f"     chains prot.: {CHAINS_PROTOCOLS}")
        logger.info(f"     excluded_hyp: {EXCLUDED_HYPERVISORS}")

        # start time log
        _startime = datetime.now(timezone.utc)

        asyncio.run(feed_all())

        # end time log
        logger.info(
            f" took {get_timepassed_string(_startime)} to complete the sequencer feed"
        )

    else:
        # actual feed
        logger.info(" Starting loop feed  ")

        # create event loop
        loop = asyncio.new_event_loop()

        # create cron jobs ( utc timezone )
        crons = {}
        for function, formats in EXPR_FORMATS.items():
            for key, cron_ex_format in EXPR_FORMATS[function].items():
                args = EXPR_ARGS.get(function, {}).get(key)
                crons[f"{function}_{key}"] = crontab(
                    cron_ex_format,
                    func=EXPR_FUNCS[function],
                    args=args or (),
                    loop=loop,
                    start=True,
                    tz=timezone.utc,
                )

        # run forever
        asyncio.set_event_loop(loop)
        loop.run_forever()
