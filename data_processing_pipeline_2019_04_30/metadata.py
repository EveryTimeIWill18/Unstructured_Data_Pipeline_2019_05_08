"""
metadata
~~~~~~~~
Load in the metadata file to configure updates.
"""
import os
import abc
import xlrd
import pickle
import tempfile
import pandas as pd
from pprint import pprint
from datetime import datetime, timedelta
from data_processing_pipeline_2019_04_30.configuration import (
    prod_delta, prod_historical, pickle_path, prod_delta_last_used,
    prod_delta_metadata
)

from data_processing_pipeline_2019_04_30.data_preprocessing import load_serialized_data


pd.set_option("display.max_rows", 101)
pd.set_option("display.max_columns", 50)


# used as a test of the metadata class
TEST_METADATA_FILE = 'V:\\Prod\\Delta\\20190502\\Metadata\\20190502.xls'




# Interfaces #
####################################################################################################
class MetaDataInterface(metaclass=abc.ABCMeta):
    """An interface for loading the metadata files."""

    @abc.abstractmethod
    def load_metadata(self, file_path: str):
        """Load in the metadata file(s)."""
        pass

    @abc.abstractmethod
    def load_metadata_file(self, full_file_path: str) -> pd.DataFrame:
        """Load a specific metadata file.
        NOTE:
            Method should be used only to check output.
            Should not be used within the data piepline.
        """
        pass

    @abc.abstractmethod
    def load_backlogged_metadata(self, files_path: str, days: int):
        """Load in the backlogged metadata files."""
        pass

    @abc.abstractmethod
    def update_metadata_log(self, file_path: str):
        """Update the metadata log file that keeps track of the
        last read delta file."""
        pass

    @abc.abstractmethod
    def load_metadata_log(self, file_path: str):
        """Load in the metadata log."""
        pass


# Concrete Implementations #
####################################################################################################
class LoadMetaData(MetaDataInterface):
    """Load in the metadata file"""
    def __init__(self):
        self.START_DATE: str = '20190502'  # first delta file in the filesystem
        self.METADATA_LOG: str = 'metadata_files_log.csv'   # name of the meta data file log
        self.last_used_metadata_file: str = None
        self.current_metadata_file: pd.DataFrame = None

    def load_metadata(self, file_path: str):
        try:
            metadata_filename = str(datetime.today() - timedelta(days=1))[:10].replace('-', '')
            for _, _, files in os.walk(os.path.join(file_path, metadata_filename)):
                if metadata_filename + '.xls' in files:
                    self.current_metadata_file = pd.read_excel(
                        xlrd.open_workbook(
                            filename=os.path.join(
                                file_path, metadata_filename,
                                'Metadata', metadata_filename + '.xls'
                            ),
                            encoding_override="cp1252"
                        )
                    )
                    self.last_used_metadata_file = metadata_filename
        except:
            pass

    def load_metadata_file(self, full_file_path: str) -> pd.DataFrame:
        """
        NOTE:
            Method should be used only to check output.
            Should not be used within the data pipeline.
        """
        try:
            if os.path.isfile(full_file_path) and os.path.splitext(full_file_path)[-1] == '.xls':
                metadata_file = pd.read_excel(
                    xlrd.open_workbook(filename=full_file_path, encoding_override="cp1252")
                )
                return metadata_file
            else:
                raise OSError(f'OSError: File: {os.path.basename(full_file_path)} not found.')
        except OSError as e:
            print(e)



    def load_backlogged_metadata(self, file_path: str, days: int):
        try:
            current = os.listdir(os.path.join(file_path))
            backlog = [str(datetime.today() - timedelta(days=d))[:10].replace('-', '')
                            for d in reversed(range(1, days+1, 1))]
            return backlog
        except:
            pass

    def update_metadata_log(self, file_path: str):
        try:
            if os.path.isdir(file_path):
                if self.METADATA_LOG in os.listdir(file_path):
                    # append to the metadata log
                    with open(os.path.join(file_path, self.METADATA_LOG), 'a') as f:
                        f.write(self.last_used_metadata_file)
                else:
                    # create the metadata log for the first time
                    with open(os.path.join(file_path, self.METADATA_LOG), 'w') as f:
                        f.write(self.last_used_metadata_file)
            return self.last_used_metadata_file
        except Exception as e:
            print(e)

    def load_metadata_log(self, file_path: str):
        try:
            if os.path.isdir(file_path):
                if len(os.listdir(file_path)) > 0:
                    pass
                else:
                    print("No metadata files to ")
        except:
            pass






class MetaData(MetaDataInterface):
    """A concrete implementation of MetaDataInterface"""

    def __init__(self):
        self.meta_data_files: list = []
        self.meta_data_df: pd.DataFrame = None
        self.START_DATE: str = '20190502'   # first delta file in the filesystem
        self.backlog_date: str = str(datetime.today() - timedelta(days=3))[:10].replace('-', '')
        self.current_date: str = str(datetime.today())[:10].replace('-', '')


    def load_metadata(self, file_path: str):
        try:
            current = os.listdir(os.path.join(file_path, self.START_DATE))
            for _, _, files in os.walk(os.path.join(file_path, self.START_DATE)):
                if self.START_DATE + '.xls' in files:
                    book = xlrd.open_workbook(
                        os.path.join(file_path, self.START_DATE, 'Metadata', self.START_DATE + '.xls'),
                        encoding_override="cp1252"
                    )
                    self.meta_data_df = pd.read_excel(book)
        except:
            pass

    def load_backlogged_metadata(self, files_path: str, days: int):
        try:
            for d in os.listdir(files_path):
                try:
                    print(d)
                    current = os.path.join(files_path, d, 'Metadata', d + '.xls')
                    book = xlrd.open_workbook(current, encoding_override="cp1252")
                    self.meta_data_files.append(pd.read_excel(book,
                        # usecols=['Object_name','Prior_Version_Object_Name',
                        #          'Document_date', 'Business_segment',
                        #          'new/changed','claim_id', 'cedent_id']
                    ))
                except Exception as e:
                    print(e)
        except OSError as e:
            print(e)

    def update_metadata_log(self, file_path: str):
        pass




def main():
    metadata = LoadMetaData()
    metadata_df = metadata.load_metadata_file(full_file_path=TEST_METADATA_FILE)
    pprint(metadata_df.head())
    #metadata.load_metadata(file_path=prod_delta)
    #metadata.update_metadata_log(file_path=prod_delta_last_used)
    #print(metadata.current_metadata_file.head())
    #print(metadata.load_backlogged_metadata(file_path=prod_delta, days=5))

    # pickle_data = load_serialized_data(file_path=pickle_path, file_name='Docx_2019_04_24.pickle')
    # metadata = MetaData()
    # #metadata.load_metadata(file_path=prod_delta)
    # metadata.load_backlogged_metadata(files_path=prod_delta, days=1)
    #
    # metadata_df = pd.concat(metadata.meta_data_files, ignore_index=True)
    # # get the changed files
    # changed_df = metadata_df[(metadata_df['new/changed'] == 'Changed')]
    # # get the prior obj name that's not NaN
    # prior_df = metadata_df[(metadata_df.Prior_Version_Object_Name != 'NaN')]
    #
    # print(f"changed_df\n=========\nShape: {changed_df.shape}")
    # print(f"prior_df\n=========\nShape: {prior_df.shape}")


    # print("Pickle Data\n============")
    # pickle_data.update({'files': pickle_data['files'].str.replace('.docx', '')})
    # #pickle_data.columns = ['Prior_Version_Object_Name',  'raw_text']
    #
    # merged_df = pd.merge(left=pickle_data, right=metadata_df,
    #                      left_on='files', right_on='Prior_Version_Object_Name',
    #                      how='inner')
    # print(merged_df)

    # pkl_filnames = pickle_data.files.str.replace('.csv', '')
    #print(pkl_filnames.head(10))
    #doc_pkl_file.update({'files': doc_pkl_file['files'].str.replace('csv', 'doc')})
    #doc_mapping_file.update({'files': doc_mapping_file['files'].str.replace('csv', 'doc')})



if __name__ == '__main__':
    main()


