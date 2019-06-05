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


class Report:

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

        if reportType == "custom":
            self.payload = json.loads(custom_payload)
        else:
            self.payload = self.loadPayload(reportType)

        if "startDate" in self.payload:
            self.payload["startDate"] = self.startDate  # + "T00:00:00.000Z"
        if "endDate" in self.payload:
            self.payload["endDate"] = self.endDate  # + "T00:00:00.000Z"

        self.request_status = 1

        if active_restaurant_loop:
            logging.info("Active Restaurants Loop: Enabled")
            with open("active_restaurant.json", "r") as f:
                json_list = json.load(f)

            self.active_restaurant_list = json_list["active_restaurant"]
            self.failed_list = []

            for i in self.active_restaurant_list:
                logging.info("Processing Restaurant: {0}({1})".format(
                    i["name"], i["id"]))
                self.payload["locationGroupID"] = i["id"]

                logging.info("Payload Submitted: {0}".format(self.payload))
                self.request_status = self.ExtractText()
                if self.request_status == 0:
                    logging.error(
                        "Restaurant {0}({1}): REQUEST FAILED...".format(i["name"], i["id"]))
                    self.failed_list.append(i)
                else:
                    self.output_1cell(self.filename)
            logging.info(
                "List of failed request restaurant: {0}".format(self.failed_list))
        else:
            logging.info("Payload Submitted: {0}".format(self.payload))
            self.request_status = self.ExtractText()
            if self.request_status == 0:
                raise Exception("Request Failed...")
            self.output_1cell(self.filename)

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

    def PostReport(self):
        """
        Post request with given payload
        """

        url = "https://api.ctuit.com/api/Report/Queue"

        response = requests.request("POST", url, data=json.dumps(
            self.payload), headers=self.headers)
        # Exiting the run if there is POST error
        if response.status_code == 500:
            logging.error("POST Return: {}".format(response.text))
            logging.error("POST Report Status: {}".format(response.status_code))
            sys.exit(1)
        logging.info("POST Status: {0}".format(response.status_code))
        logging.info("POST Return: {0}".format(response.text))

        # print(response)
        if response.json()["isValid"] is True:
            self.reportID = str(response.json()["id"])
        else:
            self.Exit = 1

    def CheckStatus(self):

        url = "https://api.ctuit.com/api/Report/Queue/" + self.reportID

        response = requests.request("GET", url, headers=self.headers)

        self.status = str(response.json()["status"])
        logging.info("Status Check: {0}".format(response.json()))

    def GetContent(self):

        url = "https://api.ctuit.com/api/Report/Queue/" + self.reportID + "/Content"

        querystring = {"contentDisposition": "2"}

        response = requests.request(
            "GET", url, headers=self.headers, params=querystring)

        if response.status_code != 200:
            logging.error("GetContent Status: {}".format(response.status_code))
            logging.error("GetContent Error Message: {}".format(response.text))
            sys.exit(1)
        logging.info("GetContent Status: {}".format(response.status_code))

        self.content = response.text

    def ExtractText(self):

        self.PostReport()
        # sys.exit(0)
        time.sleep(10)
        self.CheckStatus()

        tries = 0
        while tries < 10 and self.status != 3:
            logging.info("Number of Tries: {0}".format(tries))
            time.sleep(10)
            self.CheckStatus()
            if self.status == "1" or self.status == "2":
                logging.info("Status: {0} - Pending...".format(self.status))
                # self.CheckStatus()
            if self.status == "3":
                tries = 100
            if self.status == "5":
                # return "Shit's hit the fan fam, status = 5"
                logging.error("Status: {0} - Error...".format(self.status))
                #raise Exception("Request parameters are not valid. Please validate.")
                return 0
            tries += 1

        if self.status == "3":
            logging.info(
                "Status: {0} - Fetching and Parsing results...".format(self.status))
            self.GetContent()

        else:
            # return "Gave up. Status was" + " " + self.status
            # raise Exception( "Gave up. Status was {0}".format(self.status))
            logging.error("Gave up. Status was {0}".format(self.status))
            return 0

        return 1

    def output_1cell(self, filename):
        """
        Output everything into one cell 
        """

        date_concat = "{0} to {1}".format(self.startDate, self.endDate)
        if active_restaurant_loop:
            column_name = ["range", "start_date",
                           "end_date", "location_id", "content"]
            data = [date_concat, self.startDate, self.endDate, str(
                self.payload["locationGroupID"]), "{0}".format(self.content)]
            data_out = [column_name, data]
        else:
            column_name = ["range", "start_date", "end_date", "content"]
            data = [date_concat, self.startDate,
                    self.endDate, "{0}".format(self.content)]
            data_out = [column_name, data]

        # If active restaurant loop is true
        if not os.path.isfile(filename):
            with open(filename, "w") as f:
                writer = csv.writer(f)
                #writer.writerow(["range", "start_date", "end_date", "content"])
                #writer.writerow([date_concat, start_date, end_date, "{0}".format(self.content)])
                writer.writerows(data_out)
                # f.write(["content"])
                # f.write(["{0}"].format(self.content))
            f.close()
        else:
            with open(filename, "a") as f:
                writer = csv.writer(f)
                writer.writerows([data])
            f.close()

        logging.info("Outputting... ")
        self.produce_manifest(filename)

    def produce_manifest(self, file_name):
        """
        Dummy function to return header per file type.
        """

        #file = "/data/out/tables/"+str(file_name)+".manifest"
        file = file_name+".manifest"
        destination_part = file_name.split(".csv")[0]

        manifest_template = {  # "source": "myfile.csv"
            # ,"destination": "in.c-mybucket.table"
            "incremental": True, "primary_key": ["range"]
            # ,"columns": [""]
            # ,"delimiter": "|"
            # ,"enclosure": ""
        }
        if active_restaurant_loop:
            manifest_template["primary_key"] = ["range", "location_id"]
        #column_header = []

        try:
            with open(file, 'w') as file_out:
                json.dump(manifest_template, file_out)
                logging.info(
                    "Output manifest file - {0} - produced.".format(file))
        except Exception as e:
            logging.error("Could not produce output file manifest.")
            logging.error(e)

        return

