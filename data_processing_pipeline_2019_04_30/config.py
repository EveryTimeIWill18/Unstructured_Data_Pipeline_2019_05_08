"""
config
~~~~~~
"""
import os
from configparser import ConfigParser

# path to the configuration file
config_file = "C:\\Users\\wmurphy\\PycharmProjects\\Data_Processing_Pipeline_2019_4_30\\_config.ini"

# create a ConfigParser instance
config = ConfigParser()
config.read(config_file)
sections = config.sections()

# sftp setup
host = config.get(sections[0], 'HOST')
port = config.get(sections[0], 'PORT')
username = config.get(sections[0], 'USERNAME')
password = config.get(sections[0], 'PASSWORD')


# raw data
personal_umbrella = config.get(sections[1], 'PERSONAL_UMBRELLA')
sa_claims = config.get(sections[1], 'SA_CLAIMS')
global_business_data = config.get(sections[1], 'GLOBAL_BUSINESS_DATA')
prod = config.get(sections[1], 'PROD')
prod_historical = config.get(sections[1], 'HISTORICAL')
prod_delta = config.get(sections[1], 'DELTA')
dev = config.get(sections[1], 'DEV')
dev_dev = config.get(sections[1], 'DEV_DEV')
dev_historical = config.get(sections[1], 'DEV_HISTORICAL')
prod_delta_metadata = config.get(sections[1], 'PROD_DELTA_METADATA')
#prod_delta_last_used = config.get(sections[1], 'prod_delta_last_used'.upper())

# pickle path
pickle_path = config.get(sections[2], 'PICKLE_PATH')
pdf_pickle = config.get(sections[2], 'PDF_PICKLE_PATH')
# mapping file
mapping_file = config.get(sections[3], 'MAPPING_PATH')

# log file path
log_file_path = config.get(sections[4], 'LOG_FILE_PATH')

# error file path
error_file_path = config.get(sections[5], 'ERROR_FILE_PATH')

# write paths
eml_write_path = config.get(sections[6], 'EML_WRITE_PATH')
rtf_write_path = config.get(sections[6], 'RTF_WRITE_PATH')
doc_write_path = config.get(sections[6], 'DOC_WRITE_PATH')
docx_write_path = config.get(sections[6], 'DOCX_WRITE_PATH')
pdf_write_path = config.get(sections[6], 'PDF_WRITE_PATH')
doc_test_write_path = config.get(sections[6], 'DOC_TEST_WRITE_PATH')
#extracted_pdf_images = config.get(sections[6], 'EXTRACTED_PDF_IMAGES')

# r files
r_path = config.get(sections[7], 'R_PATH')
r_executable = config.get(sections[7], 'R_EXECUTABLE')
r_script_one = config.get(sections[7], 'R_SCRIPT_ONE')
r_script_two = config.get(sections[7], 'R_SCRIPT_TWO')
r_script_three = config.get(sections[7], 'R_SCRIPT_THREE')

claims_insights_remote_path = config.get(sections[8], 'CLAIMS_INSIGHTS_CSV_FILES')
