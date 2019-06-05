from keboola import docker
import logging_gelf.handlers
import logging_gelf.formatters
import pandas as pd
import datetime
import dateparser
import time
import requests
import json
import csv
import logging
import os
import sys

from ctuit_report import Report
from ctuit_export import Export

"__author__ = 'Leo Chan & Nico '"
"__credits__ = 'Keboola 2018'"
"__project__ = 'kbc_ctuit'"

"""
Python 3 environment 
"""


# Environment setup
abspath = os.path.abspath(__file__)
script_path = os.path.dirname(abspath)
os.chdir(script_path)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S")
"""
logger = logging.getLogger()
logging_gelf_handler = logging_gelf.handlers.GELFTCPSocketHandler(
    host=os.getenv('KBC_LOGGER_ADDR'),
    port=int(os.getenv('KBC_LOGGER_PORT'))
)
logging_gelf_handler.setFormatter(
    logging_gelf.formatters.GELFFormatter(null_character=True))
logger.addHandler(logging_gelf_handler)

# removes the initial stdout logging
logger.removeHandler(logger.handlers[0])
"""

# Access the supplied rules
cfg = docker.Config('/data/')
params = cfg.get_parameters()
userid = params["X-UserID"]
token = params["#X-UserAuthToken"]
template = params["template"]
active_restaurant_loop = bool(params["active_restaurant_loop"])
start_date = params["start_date"]
end_date = params["end_date"]
custom_payload = params["custom_payload"]  # .replace('\\n', '\n')

# Get proper list of tables
cfg = docker.Config('/data/')
in_tables = cfg.get_input_tables()
out_tables = cfg.get_expected_output_tables()
logging.info("IN tables mapped: "+str(in_tables))
logging.info("OUT tables mapped: "+str(out_tables))

# destination to fetch and output files
DEFAULT_FILE_INPUT = "/data/in/tables/"
DEFAULT_FILE_DESTINATION = "/data/out/tables/"


def get_tables(in_tables):
    """
    Evaluate input and output table names.
    Only taking the first one into consideration!
    """

    # input file
    table = in_tables[0]
    in_name = table["full_path"]
    in_destination = table["destination"]
    logging.info("Data table: " + str(in_name))
    logging.info("Input table source: " + str(in_destination))

    return in_name


def get_output_tables(out_tables):
    """
    Evaluate output table names.
    Only taking the first one into consideration!
    """

    # Output file
    table = out_tables[0]
    in_name = table["full_path"]
    in_destination = table["source"]
    #logging.info("Data table: " + str(in_name))
    logging.info("Output Table Destination: " + str(in_destination))

    return in_name


def main():
    """
    Main execution script.
    """

    # filename = DEFAULT_FILE_DESTINATION+template+".csv"
    # file_out = get_output_tables(out_tables)
    if template in ('labor_by_day'):
        Export(template, start_date, end_date)
    else:
        Report(template, start_date, end_date)
    # data_in.output_1cell(filename)

    return


if __name__ == "__main__":

    main()

    logging.info("Done.")
