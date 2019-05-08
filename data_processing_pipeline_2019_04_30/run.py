"""
run
~~~
Run the unstructured data pipeline.
"""
import os
import sys
import time
from pprint import pprint
from datetime import datetime, timedelta
# from data_processing_pipeline_2019_04_30.configuration import (
#     eml_write_path, rtf_write_path, doc_write_path, docx_write_path,
#     pdf_pickle_path, prod_delta, prod_historical, pickle_path, pdf_pickle,
#     mapping_file, claims_insights_remote_path
# )
from data_processing_pipeline_2019_04_30.config import (
    eml_write_path, rtf_write_path, doc_write_path, docx_write_path,
    prod_delta, prod_historical, pickle_path, pdf_pickle,
    mapping_file, claims_insights_remote_path, username, password, host, port,
    personal_umbrella
)


from data_processing_pipeline_2019_04_30.data_preprocessing import (
    EmlParser, RtfParser, DocParser, DocxParser, PdfParser, ParserFactory,
    load_mapping_file, load_serialized_data , write_dataframe_to_csv
)
from data_processing_pipeline_2019_04_30.server import SftpConnection
from data_processing_pipeline_2019_04_30.hive import Hive, HivCli, HDFS
from data_processing_pipeline_2019_04_30.metadata import LoadMetaData, TEST_METADATA_FILE



# Parsers #
####################################################################################################
parser_factory = ParserFactory() # create an instance of the ParserFactory class

# date configuration
d = str(datetime.today())[:10].replace("-","_")

def compare_delta_to_metadata():
    """
    INFO:
        Use to check files against metadata.
    :return:
    """


def create_hive_table(table_name: str, database: str) -> None:
    """
    Creates a Hive table with the following format:
    Columns: object_id(Integer), claim_id(Integer), filename(String), file_extension(String), extracted_text(String)
    :return:
    """

    table = Hive(username=username, password=password, host=host, port=int(port))
    # build the ExtractedText Table
    table.create_hive_table(table_name=table_name, database=database,
                            object_id='String', claim_id='String',
                            filename='String', file_extension='String',
                            extracted_text='String')

def load_data_into_hive(file_name: str, table_name: str) -> None:
    """
    Load the raw data set into the hive table.
    :param table_name:
    :param database:
    :return:
    """
    table = Hive(username=username, password=password, host=host, port=int(port))
    table.load_hive_table(file_path=claims_insights_remote_path,
                          file_name=file_name, table_name=table_name)



def run_docx_parser(raw_files: str):
    """run docx parser"""
    # extract the text and write to a pickle file
    parser_factory.parse_file_ext(file_path=raw_files, file_ext='docx')
    parser_factory.serialize_contents(write_path=pickle_path)

    # load the mapping file dataframe
    mapping_df = load_mapping_file(file_path=mapping_file, file_name='DocxMappingFile.csv')

    # load the pickle file dataframe
    f_name = 'Docx_' + d + '.pickle'
    pkl_df = load_serialized_data(file_path=pickle_path, file_name=f_name)

    # merge the dataframe
    docx_df = mapping_df.merge(pkl_df, how='inner')

    # write dataframe to csv file
    write_dataframe_to_csv(
        docx_df, write_path=docx_write_path,
        file_name='MergedDataFrame', file_ext='docx'
    )

    # wait util Docx_MergedDataFrame_2019_mm_dd exists
    docx_merged_df = 'Docx_MergedDataFrame_' + d + '.csv'
    while os.path.exists(os.path.join(docx_write_path, docx_merged_df)) == False:
        time.sleep(1)
    else:
        print(f'File: {docx_merged_df} exists.')

    # push the dataset to the linux server via sftp
    sftp_conn = SftpConnection(root_path=docx_write_path,
                                   remote_path=claims_insights_remote_path)
    sftp_conn.connect(filename=docx_merged_df, filepath=docx_write_path)

    # create a hive table for the docx dataset
    create_hive_table(table_name='personal_umbrealla_docx_5_8_2019',database='drw')
    time.sleep(5)

    # load the dataset into hive
    load_data_into_hive(file_name=docx_merged_df, table_name='personal_umbrealla_docx_5_8_2019')







def run_eml_parser(file_path: str, is_historical=False):
    """Run the eml parser"""
    if is_historical:
        pass



def main():
    run_docx_parser(raw_files=personal_umbrella)

if __name__ == '__main__':
    main()
