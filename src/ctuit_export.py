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
"__author__ = 'Leo Chan & Nico '"
"__credits__ = 'Keboola 2018'"
"__project__ = 'kbc_ctuit'"

"""
Python 3 environment 
"""


# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S")

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

# destination to fetch and output files
DEFAULT_FILE_INPUT = "/data/in/tables/"
DEFAULT_FILE_DESTINATION = "/data/out/tables/"


class Export:

    def __init__(self, reportType, startDate, endDate):
        self.Exit = 0
        self.filename = DEFAULT_FILE_DESTINATION+reportType+".csv"
        self.startDate = self.parseDate(startDate)
        self.endDate = self.parseDate(endDate)
        logging.info("startDate: {0}, endDate: {1}".format(
            self.startDate, self.endDate))
        if self.endDate < self.startDate:
            raise Exception("Please validate your date parameters.")

        self.headers = {
            'X-UserID': userid,
            'X-UserAuthToken': token,
            'Content-Type': "application/json"
        }
        self.payload = self.loadPayload(reportType)

        if "startDate" in self.payload:
            self.payload["startDate"] = self.startDate  # + "T00:00:00.000Z"
        if "endDate" in self.payload:
            self.payload["endDate"] = self.endDate  # + "T00:00:00.000Z"

        self.request_status = 1

        # Parsing endpoints
        if reportType == "labor_by_day":
            self.labor_by_day()

    def parseDate(self, date):
        """
        Parsing Input date parameter
        """

        temp = dateparser.parse(date)
        temp_date = temp.strftime("%Y-%m-%d")

        return temp_date

    def loadPayload(self, template):
        """
        Loading payload template from payload.json
        """

        logging.info("Loading Payload information...")
        with open("payload.json", "r") as f:
            json_template = json.load(f)

        payload = json_template[template]
        # payload["startDate"] = self.startDate + "T00:00:00.000Z"
        # payload["endDate"] = self.endDate + "T00:00:00.000Z"
        logging.info("Payload: {0}".format(payload))

        return payload

    def getRequest(self, url):
        """
        GET Request
        """

        response = requests.get(url=url, headers=self.headers)

        return response

    def postRequest(self, url, payload):
        """
        POST Request
        """

        response = requests.request("POST", url, data=json.dumps(
            self.payload), headers=self.headers)

        return response

    def produce_manifest(self, file_name, primary_key, columns):
        """
        Dummy function to return header per file type.
        """

        file = DEFAULT_FILE_DESTINATION+file_name+".manifest"

        manifest_template = {  # "source": "myfile.csv"
            # ,"destination": "in.c-mybucket.table"
            "incremental": True, 
            "primary_key": primary_key,
            "columns": columns,
            "delimiter": ","
            # ,"enclosure": ""
        }

        try:
            with open(file, 'w') as file_out:
                json.dump(manifest_template, file_out)
                logging.info(
                    "Output manifest file - {0} - produced.".format(file))
        except Exception as e:
            logging.error("Could not produce output file manifest.")
            logging.error(e)

        return

    def labor_by_day(self):
        """
        Fetching all the available jobs for labor by day
        restricting to 10 jobs per request due to API restrictions
        """

        details_url = "https://api.ctuit.com/api/Export/-106"
        detail_response = self.getRequest(details_url)
        if detail_response.status_code != 200:
            logging.error("Failed to fetch Labor by Day details...")
            logging.error("Please check credentials and permissions...")
            sys.exit(1)
        detail_response_json = detail_response.json()

        list_of_jobs = []
        fetch_job_bool = False
        for option in detail_response_json["options"]:
            if option["name"] == "JOBID":
                for job in option["data"]:
                    list_of_jobs.append(str(job["value"]))
                fetch_job_bool = True
        logging.info("Total JOBID: {}".format(len(list_of_jobs)))
        
        # Exiting if no JOBIDs are fetched
        if not fetch_job_bool or len(list_of_jobs) == 0:
            logging.error("Failed to fetch JobDetails from Labor by Day...")
            logging.error("Please check account permissions...")
            sys.exit(1)

        # Create Labor_by_day folder
        # path = DEFAULT_FILE_DESTINATION+"labor_by_day"
        # if not os.path.exists(path):
        #     os.makedirs(path)

        # 10 Jobs request per QUEUE due to API restriction
        job_queue_itr = 0
        expected_columns = [
            "LOCATIONID",
            "LOCATIONNAME",
            "DOB",
            "DAY_OF_WEEK",
            "GROSS_SALES",
            "NET_SALES",
            "JOBNAME",
            "EXPORTID",
            "TOTAL_LABOR_HOURS",
            "TOTAL_LABOR_DOLLARS",
            "LABOR_GROSS",
            "LABOR_NET",
            "REG_LABOR_HOURS",
            "REG_LABOR_DOLLARS",
            "OT_LABOR_HOURS",
            "OT_LABOR_DOLLARS",
            "DT_LABOR_HOURS",
            "DT_LABOR_DOLLARS"
        ]
        expected_pk = [
            "LOCATIONID",
            "JOBNAME",
            "DOB"
        ]
        with open(DEFAULT_FILE_DESTINATION+"labor_by_day.csv", "w") as out_file:
            writer = csv.writer(out_file)
            while job_queue_itr < len(list_of_jobs):
                s = ','
                job_queue = list_of_jobs[job_queue_itr: job_queue_itr + 10]
                job_queue_itr += 10
                logging.info("Parsing JOBID: {}".format(str(s.join(job_queue))))
                
                temp_payload = self.payload
                temp_payload["options"][0]["value"] = str(s.join(job_queue))

                queue_url = "https://api.ctuit.com/api/Export/Queue"
                queue_response = self.postRequest(queue_url, temp_payload)
                queue_response_json = queue_response.json()

                # Validate request status
                tries = 0
                status = queue_response_json["status"]
                report_id = queue_response_json["id"]
                status_url = "https://api.ctuit.com/api/Export/Queue/{}".format(report_id)

                while tries < 10 and status not in (3, 5):
                    time.sleep(2)
                    status_response = self.getRequest(status_url)
                    status_response_json = status_response.json()
                    status = status_response_json["status"]
                    if status == 3:
                        file_name = status_response_json["fileName"]
                        logging.info("Downloading {}...".format(file_name))
                    elif status == 5:
                        pass
                    tries += 1
                
                # Get Content
                first_line = True
                if status == 3:
                    # report_id = 87652199
                    content_url = "https://api.ctuit.com/api/Export/Queue/{}/content".format(report_id)
                    content_response = self.getRequest(content_url)
                    content_text = content_response.text
                    
                    for line in content_text.split("\n"):
                        #logging.info(line)
                        if first_line:
                            first_line = False
                            # Validate headers
                            if len(line.split(',')) != len(expected_columns):
                                logging.error("Number of expected columns have changed. Please contact support.")
                                sys.exit(1)
                        else:
                            #out_file.write(line)
                            writer.writerow(line.split(','))

            out_file.close()
        self.produce_manifest("labor_by_day.csv", expected_pk, expected_columns)

        return
