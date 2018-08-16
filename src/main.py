"__author__ = 'Leo Chan & Nico '"
"__credits__ = 'Keboola 2018'"
"__project__ = 'kbc_ctuit'"

"""
Python 3 environment 
"""

import sys
import os
import logging
import csv
import json
import requests
import time
import dateparser
import datetime
import pandas as pd
import logging_gelf.formatters
import logging_gelf.handlers
from keboola import docker

### Environment setup
abspath = os.path.abspath(__file__)
script_path = os.path.dirname(abspath)
os.chdir(script_path)

### Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S")

logger = logging.getLogger()
logging_gelf_handler = logging_gelf.handlers.GELFTCPSocketHandler(
    host=os.getenv('KBC_LOGGER_ADDR'),
    port=int(os.getenv('KBC_LOGGER_PORT'))
    )
logging_gelf_handler.setFormatter(logging_gelf.formatters.GELFFormatter(null_character=True))
logger.addHandler(logging_gelf_handler)

# removes the initial stdout logging
logger.removeHandler(logger.handlers[0])


### Access the supplied rules
cfg = docker.Config('/data/')
params = cfg.get_parameters()
userid = params["X-UserID"]
token = params["#X-UserAuthToken"]
template = params["template"]
start_date = params["start_date"]
end_date = params["end_date"]

### Get proper list of tables
cfg = docker.Config('/data/')
in_tables = cfg.get_input_tables()
out_tables = cfg.get_expected_output_tables()
logging.info("IN tables mapped: "+str(in_tables))
logging.info("OUT tables mapped: "+str(out_tables))

### destination to fetch and output files
DEFAULT_FILE_INPUT = "/data/in/tables/"
DEFAULT_FILE_DESTINATION = "/data/out/tables/"


def get_tables(in_tables):
    """
    Evaluate input and output table names.
    Only taking the first one into consideration!
    """

    ### input file
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

    ### Output file
    table = out_tables[0]
    in_name = table["full_path"]
    in_destination = table["source"]
    #logging.info("Data table: " + str(in_name))
    logging.info("Output Table Destination: " + str(in_destination))

    return in_name


class Report:
    
    def __init__(self,reportType,startDate, endDate):                    
        
        self.Exit = 0
        self.startDate = self.parseDate(startDate)
        self.endDate = self.parseDate(endDate)
        logging.info("startDate: {0}, endDate: {1}".format(self.startDate, self.endDate)) 
        if self.endDate < self.startDate:
            raise Exception("Please validate your date parameters.")
        
        self.headers = {
            'X-UserID': userid,
            'X-UserAuthToken': token,
            'Content-Type': "application/json"
            }

        self.payload = self.loadPayload(reportType)
        
        self.ExtractText()
        
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
        payload["startDate"] = self.startDate + "T00:00:00.000Z"
        payload["endDate"] = self.endDate + "T00:00:00.000Z"
        logging.info("Payload: {0}".format(payload))

        return payload

    def PostReport(self):
        """
        Post request with given payload
        """
        
        url = "https://api.ctuit.com/api/Report/Queue"

        response = requests.request("POST", url, data=json.dumps(self.payload), headers=self.headers)
        logging.info("POST Status: {0}".format(response.status_code))
        logging.info("POST Return: {0}".format(response.text))

        #print(response)
        if response.json()["isValid"]==True:            
            self.reportID = str(response.json()["id"])
        else:
            self.Exit = 1    

    def CheckStatus(self):
        
        url = "https://api.ctuit.com/api/Report/Queue/" + self.reportID
        
        response = requests.request("GET", url, headers=self.headers)

        self.status = str(response.json()["status"])

    def GetContent(self):
        
        url = "https://api.ctuit.com/api/Report/Queue/" + self.reportID + "/Content"
    
        querystring = {"contentDisposition":"2"}
    
        response = requests.request("GET", url, headers=self.headers, params=querystring)
    
        self.content = response.text
         
    def ExtractText(self):

        self.PostReport()
        #sys.exit(0)
        time.sleep(10)
        self.CheckStatus()

        tries = 0
        while tries < 3 and self.status != 3:
            time.sleep(5)
            if self.status == "2":
                logging.info("Status: {0} - Pending...".format(self.status))
                self.CheckStatus()
            if self.status == "5":
                #return "Shit's hit the fan fam, status = 5"
                logging.error("Status: {0} - Error...".format(self.status))
                raise Exception("Request parameters are not valid. Please validate.")
            tries += 1
                
        if self.status == "3":
            logging.info("Status: {0} - Fetching and Parsing results...".format(self.status))                                
            self.GetContent()

        else:
            #return "Gave up. Status was" + " " + self.status
            raise Exception( "Gave up. Status was {0}".format(self.status))

    def output_1cell(self, filename):
        """
        Output everything into one cell 
        """

        date_concat = "{0} to {1}".format(self.startDate,self.endDate)
        column_name = ["range", "start_date", "end_date", "content"]
        data = [date_concat, self.startDate, self.endDate, "{0}".format(self.content)]
        data_out = [column_name, data]
        with open(filename, "w") as f:
            writer = csv.writer(f)
            #writer.writerow(["range", "start_date", "end_date", "content"])
            #writer.writerow([date_concat, start_date, end_date, "{0}".format(self.content)])
            writer.writerows(data_out)
            #f.write(["content"])
            #f.write(["{0}"].format(self.content))
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

        manifest_template = {#"source": "myfile.csv"
                            #,"destination": "in.c-mybucket.table"
                            "incremental": True
                            ,"primary_key": ["range"]
                            #,"columns": [""]
                            #,"delimiter": "|"
                            #,"enclosure": ""
                            }

        #column_header = []

        try:
            with open(file, 'w') as file_out:
                json.dump(manifest_template, file_out)
                logging.info("Output manifest file - {0} - produced.".format(file))
        except Exception as e:
            logging.error("Could not produce output file manifest.")
            logging.error(e)
        
        return


def main():
    """
    Main execution script.
    """

    filename = DEFAULT_FILE_DESTINATION+template+".csv"
    #file_out = get_output_tables(out_tables)
    data_in = Report(template, start_date, end_date)
    data_in.output_1cell(filename)

    return


if __name__ == "__main__":

    main()
    
    logging.info("Done.")
