import configparser
import os

class config_reader:

    def get_config_pipeline(self):
        parser = configparser.ConfigParser()
        parser.read('D:\\DataEngineering_Projects\\api_extract_csv\\config\\pipeline.ini')
        return parser
