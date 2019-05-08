"""
data_preprocessing
~~~~~~~~~~~~~~~~~~
TODO:
    Mapping Dictionary Key:
        - mapping dictionary: {(object_id, claim_id, filename, previous_filename): raw_text}
"""
import os
import re
import abc

import string
import pickle
import zipfile
import subprocess
import xlsxwriter
import pandas as pd
from email import policy
from pprint import pprint
from datetime import datetime
from bs4 import BeautifulSoup
from PyPDF2 import PdfFileReader
from email.parser import BytesParser
from xml.etree.cElementTree import XML
from typing import Dict, List, Type, TypeVar, NewType, AnyStr, ByteString
from data_processing_pipeline_2019_04_30.file_encoders import (destinations, specialchars,
                           BARS, TABS, PUNCTUATION, SPACES, WHITESPACE,
                           NEWLINE, RTF_ENCODING)
from data_processing_pipeline_2019_04_30.logging_config import BaseLogger
from data_processing_pipeline_2019_04_30.configuration import (personal_umbrella, sa_claims,
        pickle_path, mapping_file, log_file_path, error_file_path,
        eml_write_path, rtf_write_path, doc_write_path,
        docx_write_path, pdf_write_path, r_path, r_executable,
                           r_script_one, r_script_two, r_script_three,
                           doc_test_write_path, pdf_pickle_path, prod_delta_metadata)

# date configuration
d = str(datetime.today())[:10].replace("-","_")

# logging configuration
logfile = "data_pipeline_log_" + d
logger = BaseLogger(log_file_path, logfile)
logger.config()     # use the default logging configuration


# Data Type Configuration #
####################################################################################################

# basic types
Path = NewType('Path', str)
File = NewType('File', str)
FileExt = NewType('FileExt', str)
Int = NewType('Int', int)
IntString = str(Int)

# data structures
MinedText = NewType('MinedText', str)
DataMapping = NewType('DataMapping', Dict[File, MinedText])


# Interfaces #
####################################################################################################
class FileParserInterface(metaclass=abc.ABCMeta):
    """File Parser Interface"""

    @abc.abstractmethod
    def extract_text(self, current_file):
        """Extract text from the current file."""
        pass

    @abc.abstractmethod
    def load_metadata(self, file_path, metadata_file):
        """Load the metadata file that contains the
        file's previous filename.
        """
        pass


class FileGenerator(object):
    """File Generator Interface"""

    def __init__(self, file_path, file_ext):
        self.file_path = file_path
        self.file_ext = file_ext

    def __iter__(self):
        try:
            if os.path.isdir(self.file_path):
                for name in os.scandir(self.file_path):
                    if os.path.splitext(os.path.basename(name))[-1] == '.'+self.file_ext:
                        yield os.path.join(self.file_path, os.path.basename(name))
        except StopIteration:
            print("Finished processing files")


# Concrete Parsers #
####################################################################################################
class EmlParser(FileParserInterface):
    """Eml File Parser"""

    def __init__(self, file_path):
        self.file_path = file_path
        self.mapping_dict: DataMapping = {}        # {(object_id, claim_id, filename, previous_filename): raw_text}

        # file counters
        self.file_counter: int = 0          # count of the files successfully parsed
        self.error_file_counter: int = 0    # count of files that raised errors
        self.error_files: list = []

        # logging configuration
        logger.info(info='starting eml parsing')

    def extract_text(self, current_file) -> dict:
        """Extract the current email's text"""
        try:
            with open(current_file, 'rb') as eml_f:
                msg = BytesParser(policy=policy.default).parse(eml_f)
                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_type() == 'text/html':
                            soup = BeautifulSoup(part.get_content(), 'html.parser')
                            body = soup.findAll(text=True)  # extract the text
                            # process the text list into a formatted string
                            body = ' '.join(body) \
                                .translate(str.maketrans('', '', string.punctuation)) \
                                .lower()
                            self.mapping_dict.update({os.path.basename(current_file): body})
                            self.file_counter += 1
                            return {os.path.basename(current_file): body}
        except OSError as e:
            if current_file in self.error_files:
                pass
            else:
                self.error_file_counter += 1
                self.error_files.append(os.path.basename(current_file))  # added: 4/16/2019
                logger.error(error=f'OSError: Could not parse email: {os.path.basename(current_file)}')
                logger.error(error=f"Python Exception: {e}") # added: 5/1/2019
        except Exception as e:  # added: 5/1/2019
            if current_file in self.error_files:
                pass
            else:
                self.error_file_counter += 1
                self.error_files.append(os.path.basename(current_file))
                logger.error(error=f"Python Exception: {e}")

    def load_metadata(self, file_path: str, metadata_file: str):
        """Load the metadata file that contains the
        file's previous filename.
        """
        pass

class PdfParser(FileParserInterface):
    """Pdf File Parser
    If the content of the pdf is an empty string
    use pytesseract. else: user PyPDF2
    """
    def __init__(self, file_path):
        self.file_path = file_path
        self.mapping_dict: DataMapping = {}
        self.pdf_content_by_page: List[Dict[AnyStr]] = []
        self.pdf_by_page_counter: int = 0

        # file counters
        self.file_counter: int = 0  # count of the files successfully parsed
        self.error_file_counter: int = 0  # count of files that raised errors
        self.error_files: list = []

        # logging configuration
        logger.info(info='starting pdf parsing')

    def extract_text_with_adobe(self):
        """Extract text using adobe Acrobat"""

    def extract_text(self, current_file):
        try:
            # add current pdf to pdf_content_by_page
            self.pdf_content_by_page.append({'filename': current_file})  # added 5/2/2019

            with open(current_file, 'rb') as f:
                pdf_reader = PdfFileReader(f)
                pages = []
                pg = 0
                while pg < pdf_reader.numPages:
                    try:
                        current_page = pdf_reader.getPage(pg)

                        # extract the contents of the pdf
                        text = list(str(current_page.extractText()).splitlines())
                        text = ''.join(text)

                        # process the text
                        text = re.sub('(\s+|\t|\r|\n)', ' ', text) \
                            .strip() \
                            .lower()

                        # remove punctuation from text
                        text = text.translate((None, PUNCTUATION))
                        text = ' '.join(text.split())

                        # update the pdf_content_by_page
                        self.pdf_content_by_page[self.pdf_by_page_counter].update({f"page {pg}": text}) # added 5/2/2019
                        #self.pdf_content_by_page[current_file].update({pg: text}) # added 5/2/2019

                        pages.append(text)
                        pg += 1  # increment the page counter

                    except Exception as e:
                        logger.error(error=f"Error reading page: {pg} "
                                    f"from file {os.path.basename(current_file)}")
                        logger.error(error=f"Python Exception: {e}")
                        self.error_file_counter += 1
                        self.error_files.append(os.path.basename(current_file))
                        # increment the page number
                        pg += 1  # skip the page that raised an error

                # increment the page counter
                self.pdf_by_page_counter += 1   # added 5/2/2019
                self.mapping_dict.update({os.path.basename(current_file): ''.join(pages)})
                self.file_counter += 1
                return {os.path.basename(current_file): ''.join(pages)}
        except OSError as e:
            if current_file in self.error_files:
                pass
            else:
                self.error_file_counter += 1
                self.error_files.append(os.path.basename(current_file))
                logger.error(error=f"OSError: "
                f"Could not parse email: {os.path.basename(current_file)}")
                logger.error(error=f"Python Exception: {e}")
        except Exception as e:
            if current_file in self.error_files:
                pass
            else:
                self.error_file_counter += 1
                self.error_files.append(os.path.basename(current_file))
                logger.error(error=f"An error has occurred while "
                f"trying to parse file {current_file}")
                logger.error(error=f"Python Exception: {e}")

    def load_metadata(self, file_path: Path, metadata_file: File):
        """Load the metadata file that contains the
        file's previous filename.
        """
        pass


class TxtParser(FileParserInterface):
    """Text File Parser"""

    def __init__(self, file_path):
        self.file_path = file_path
        self.mapping_dict: DataMapping = {}

        # file counters
        self.file_counter: int = 0          # count of the files successfully parsed
        self.error_file_counter: int = 0    # count of files that raised errors
        self.error_files: list = []

        # logging configuration
        logger.info(info='starting txt(doc) parsing')

    def extract_text(self, current_file):
        try:
            with open(current_file, 'r', errors='replace', encoding='utf-8') as f:
                text = f.read()
                # text =  text.encode('utf-8')
                text = SPACES.sub(" ", text)
                text = text[6:]
                text = BARS.sub("", text)
                text = NEWLINE.sub(" ", text)
                text = TABS.sub(" ", text)
                text = text.translate(str.maketrans('', '', string.punctuation)).lower()
                self.mapping_dict.update({os.path.basename(current_file): text})
                self.file_counter += 1
                return {os.path.basename(current_file): text}
        except OSError:
            if current_file not in self.error_files:
                self.error_file_counter += 1
                self.error_files.append(os.path.basename(current_file))  # added: 4/16/2019
                logger.error(error=f"OSError: Could not parse email: {os.path.basename(current_file)}")

    def load_metadata(self, file_path, metadata_file):
        """Load the metadata file that contains the
        file's previous filename.
        """
        pass

class RtfParser(FileParserInterface):
    """Rtf File Parser"""
    def __init__(self, file_path):
        self.file_path = file_path
        self.mapping_dict: DataMapping = {}

        # file counters
        self.file_counter: int = 0  # count of the files successfully parsed
        self.error_file_counter: int = 0  # count of files that raised errors
        self.error_files: list = []

    def extract_text(self, current_file) -> dict:
        """Extract the current rtf file's text"""
        try:
            with open(current_file, 'rb') as f:
                text = f.read().decode('utf-8')
                stack = []
                ignorable = False
                ucskip = 1
                curskip = 0
                out = []  # Output buffer.
                for match in RTF_ENCODING.finditer(text):
                    word, arg, hex, char, brace, tchar = match.groups()
                    if brace:
                        curskip = 0
                        if brace == '{':
                            # Push state
                            stack.append((ucskip, ignorable))
                        elif brace == '}':
                            # Pop state
                            ucskip, ignorable = stack.pop()
                    elif char:  # \x (not a letter)
                        curskip = 0
                        if char == '~':
                            if not ignorable:
                                out.append('\xA0')
                        elif char in '{}\\':
                            if not ignorable:
                                out.append(char)
                        elif char == '*':
                            ignorable = True
                    elif word:  # \foo
                        curskip = 0
                        if word in destinations:
                            ignorable = True
                        elif ignorable:
                            pass
                        elif word in specialchars:
                            out.append(specialchars[word])
                        elif word == 'uc':
                            ucskip = int(arg)
                        elif word == 'u':
                            c = int(arg)
                            if c < 0: c += 0x10000
                            if c > 127:
                                out.append(chr(c))  # NOQA
                            else:
                                out.append(chr(c))
                            curskip = ucskip
                    elif hex:  # \'xx
                        if curskip > 0:
                            curskip -= 1
                        elif not ignorable:
                            c = int(hex, 16)
                            if c > 127:
                                out.append(chr(c))  # NOQA
                            else:
                                out.append(chr(c))
                    elif tchar:
                        if curskip > 0:
                            curskip -= 1
                        elif not ignorable:
                            out.append(tchar)
                    result = ''.join(out)
                result = ''.join(ch for ch in result if ch not in PUNCTUATION)
                result = SPACES.sub(" ", result)
                result = ''.join(result)

                # update self.fileDict
                self.mapping_dict.update({os.path.basename(current_file): result.lower()})
                self.file_counter += 1
                return {os.path.basename(current_file): result.lower()}
        except OSError as e:
            self.file_counter += 1
            self.error_files.append(os.path.basename(current_file))  # added: 4/16/2019
            logger.error(error=f"OSError: Could not parse email: {os.path.basename(current_file)}")
            logger.error(error=f"Python Exception: {e}")

    def load_metadata(self, file_path, metadata_file):
        """Load the metadata file that contains the
        file's previous filename.
        """
        pass

class DocxParser(FileParserInterface):
    """Docx File Parser"""

    def __init__(self, file_path):
        self.file_path = file_path
        self.mapping_dict: Dict[AnyStr] = {}

        # file counters
        self.file_counter: int = 0  # count of the files successfully parsed
        self.error_file_counter: int = 0  # count of files that raised errors
        self.error_files: list = []

        # logging configuration
        logger.info(info='starting docx parsing')

    def extract_text(self, current_file):
        """Extract the current docx file's text"""
        try:
            WORD_NAMESPACE = '{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
            PARA = WORD_NAMESPACE + 'p'
            TEXT = WORD_NAMESPACE + 't'

            # unzip the current document and read its contents
            if os.path.getsize(current_file) > 0:
                document = zipfile.ZipFile(current_file)
                for i, _ in enumerate(document.infolist()):
                    if document.infolist()[i].filename == 'word/document.xml':
                        xml_content = document.read('word/document.xml')
                        document.close()
                        tree = XML(xml_content)  # parse the xml document
                        paragraphs = []
                        for paragraph in tree.getiterator(PARA):
                            texts = [
                                node.text
                                for node in paragraph.getiterator(TEXT)
                                if node.text
                            ]
                            if texts:
                                # process the text list into a formatted string
                                texts = ' '.join(texts) \
                                    .translate(str.maketrans('', '', string.punctuation)) \
                                    .lower()
                                paragraphs.append(texts)
                        self.mapping_dict.update({os.path.basename(current_file): ''.join(paragraphs)})
                        # increment the file counter
                        self.file_counter += 1
                        return {os.path.basename(current_file): ''.join(paragraphs)}
                else:
                    pass
            else:
                self.error_file_counter += 1
                self.error_files.append(os.path.basename(current_file))  # added: 4/16/2019
                logger.error(error=f'File: {current_file} is empty')
                logger.error(error=f'File: {current_file} is empty')    # added: 5/1/2019
        except OSError as e:
            self.error_file_counter += 1
            self.error_files.append(os.path.basename(current_file))  # added: 4/16/2019
            logger.error(error=f'OSError: Could not parse email: {os.path.basename(current_file)}')
            logger.error(error=f"Python Exception: {e}")
        except Exception as e:
            self.error_file_counter += 1
            self.error_files.append(os.path.basename(current_file))  # added: 4/16/2019
            logger.error(error=f"File: {current_file} raised an error")
            logger.error(error=f"Python Exception: {e}")    # added: 5/1/2019

    def load_metadata(self, file_path: Path, metadata_file: File):
        """Load the metadata file that contains the
        file's previous filename.
        """
        pass

class DocParser(FileParserInterface):
    """Doc File Parser"""

    def __init__(self, file_path):
        self.file_path = file_path
        self.mapping_dict: Dict[AnyStr] = {}

        # file counters
        self.file_counter: int = 0  # count of the files successfully parsed
        self.error_file_counter: int = 0  # count of files that raised errors
        self.error_files: list = []

        # logging configuration
        logger.info(info='starting txt(doc) parsing')

    def run_r_script(self):
        """run the R script that converts .doc to .csv"""
        subprocess.call([r_executable, os.path.join(r_path, r_script_one)], shell=True)


    def run_doc_to_csv_rscript(self, file_path, timeout):
        """run the R script that converts .doc to .csv"""
        filepath = '"' + file_path + '"'
        #writepath = '"' + str(write_path) + '"'
        time_out = '"' + timeout + '"'

        # run the R script
        subprocess.call([r_executable,
                         os.path.join(r_path, r_script_three),
                         filepath, time_out], shell=True)

    def extract_text(self, current_file: str):
        """extract the contents from the converted .doc files"""
        try:
            with open(current_file, 'r', errors='replace', encoding='utf-8') as f:
                text = f.read()
                # text =  text.encode('utf-8')
                text = SPACES.sub(" ", text)
                text = text[6:]
                text = BARS.sub("", text)
                text = NEWLINE.sub(" ", text)
                text = TABS.sub(" ", text)
                text = text.translate(str.maketrans('', '', string.punctuation)).lower()
                self.mapping_dict.update({os.path.basename(current_file): text})
                self.file_counter += 1
                return {os.path.basename(current_file): text}
        except OSError:
            if current_file not in self.error_files:
                self.error_file_counter += 1
                self.error_files.append(os.path.basename(current_file))  # added: 4/16/2019
                logger.error(error=f"OSError: Could not parse doc: {os.path.basename(current_file)}")

    def load_metadata(self, file_path: str, metadata_file: str):
        """Load the metadata file that contains the
        file's previous filename.
        """
        pass


# file transformation functions #
####################################################################################################
def load_serialized_data(file_path: str, file_name: str) -> pd.DataFrame:
    """Load preprocessed data"""
    try:
        if os.path.isdir(file_path):
            if os.path.isfile(os.path.join(file_path,file_name)):
                try:
                    os.chdir(file_path)

                    data = pickle.load(open(file_name, "rb"))
                    s1 = pd.Series(data=list(data.keys()), name='files')
                    s2 = pd.Series(data=list(data.values()), name='raw_text')
                    # convert to a pandas DataFrame
                    df = s1.to_frame().join(s2.to_frame())
                    return df
                except pickle.PicklingError:
                    print(f"PicklingError: could not load pickle file: {file_name}")
    except OSError:
        print(f"OSError: Could not load file: {file_name}")


def load_mapping_file(file_path: str, file_name: str) -> pd.DataFrame:
    """load in the mapping file"""
    try:
        if os.path.isdir(file_path):
            if os.path.isfile(os.path.join(file_path, file_name)):
                with open(os.path.join(file_path, file_name), 'r', errors='replace') as f:
                    # process the mapping file
                    mapping_file = f.read().splitlines()
                    mapping_matrix = [row.split(',', 1) for row in mapping_file[1:]]
                    columns = mapping_file[0].split(',')

                    # convert mapping file into a pandas DataFrame
                    mapping_df = pd.DataFrame(data=mapping_matrix,
                                              columns=[columns[0], columns[1]], dtype='str')
                    return mapping_df
            else:
                print(f"{file_name} not found")
        else:
            print(f"{file_path} not found")
    except OSError:
        print(f"Path: {file_path} could not be found")


def write_dataframe_to_csv(df: pd.DataFrame, write_path: str, file_name: str, file_ext: str) -> None:
    """Write the data frame to a csv file"""
    try:
        if os.path.isdir(write_path):
            d = str(datetime.today())[:10].replace('-','_')
            name = file_ext.title() + '_' + file_name + '_' + d + '.csv'

            # write the data frame to a csv file. Remove index from the csv file output
            df.to_csv(os.path.join(write_path, name), index=False)
    except:
        print("Error writing pandas DataFrame to a csv file.")


def file_system_file_types(file_path: str) -> set:
    """Extract a set of file types from the current path"""
    try:
        if os.path.isdir(file_path):
            # get the unique set of file types
            all_file_types = set(
                list(filter(lambda x: len(x) >= 3 and len(x) <= 5,
                            list(map(lambda y: os.path.splitext(y)[-1],
                                     os.listdir(file_path)))))
            )
            return all_file_types
        else:
            raise OSError(f"OSError: file path: {file_path} does not exist.")
    except OSError as e:
        print(e)






# Factory Design Pattern #
####################################################################################################
class ParserFactory:
    """Factory class for building the file parsers"""
    file_parser: dict = None  # stores the final parsed dictionary
    file_extensions: list = ['doc', 'docx', 'eml', 'pdf', 'rtf']
    file_ext: str = None  # stores the current file extension
    #book = xlwt.Workbook(encoding='utf-8')  # excel workbook to store pipeline's results
    book = xlsxwriter.Workbook()
    current_parser_obj = None   # stores the current instance of the Parse class

    def parse_file_ext(self, file_ext: str, file_path: str):
        """Parse the file ext type"""
        if file_ext in ParserFactory.file_extensions:
            ParserFactory.file_ext = file_ext
            # if file_ext  == 'csv':
            #     # special case
            #     parser = TxtParser(file_path=file_path)
            #     parser_generator = FileGenerator(file_path=file_path, file_ext='csv')

            if file_ext == 'doc':    # added 5/2/2019
                # special case
                parser = DocParser(file_path=file_path)
                ParserFactory.current_parser_obj = parser
                parser.run_doc_to_csv_rscript(file_path=file_path, timeout=str(20))

                # doc parser_generator should access the converted doc files
                parser_generator = FileGenerator(file_path=doc_test_write_path, file_ext='csv')
                # begin iteration
                parser_iterator = parser_generator.__iter__()
                while True:
                    try:
                        parser.extract_text(next(parser_iterator))
                    except StopIteration:
                        logger.info(info=f"Finished processing {file_ext} files.")
                        break

            else:
                # load the correct file parser
                parser = globals()[f'{file_ext.title()}Parser'](file_path)
                # load the parser generator
                parser_generator = FileGenerator(file_path=file_path, file_ext=file_ext)

                # begin iteration
                parser_iterator = parser_generator.__iter__()
                while True:
                    try:
                        parser.extract_text(next(parser_iterator))
                    except StopIteration:
                        logger.info(info=f"Finished processing {file_ext} files.")
                        break

            # write the pdf_by_page list to a pickle file
            if file_ext == 'pdf':   # 5/2/2019
                pdf_by_pg_name =  ParserFactory.file_ext.title() + '_ByPage' + '_' + d + '.pickle'
                os.chdir(pdf_pickle_path)
                pickle.dump(parser.pdf_content_by_page, open(pdf_by_pg_name, "wb"))

            # write the files that raised an error to an error file
            err_file = 'ErrorFile' + file_ext.title() + '_' + d + '.csv'
            err_df = pd.DataFrame({file_ext.title() + 'ErrorFiles': parser.error_files})
            err_df.to_csv(path_or_buf=os.path.join(error_file_path, err_file))

            # load the file ext dictionary
            ParserFactory.file_parser = parser.mapping_dict

            # write the number of successes and failures
            logger.info(info=f"{file_ext.title()}: Number of successes: {parser.file_counter}")
            logger.info(info=f"{file_ext.title()}: Number of failures: {parser.error_file_counter}")

    def serialize_contents(self, write_path: str):
        # create the file name
        pkl_name = ParserFactory.file_ext.title() + '_' + d + '.pickle'
        try:
            os.chdir(write_path)
            pickle.dump(ParserFactory.file_parser, open(pkl_name, "wb"))
        except pickle.PicklingError:
            logger.error(error=f"PicklingError: An error occurred while "
            f"trying to serialize the {ParserFactory.file_ext} dictionary.")
            print("PicklingError: Could not pickle the file\n")


    def parser_results_xlsx(self, file_ext: str):  # added 5/2/2019
        """write the results of the parser to an excel spreadsheet"""
        # book = xlsxwriter.Workbook()
        wb_path = 'Y:\\Shared\\USD\\Business Data and Analytics\\Claims_Pipeline_Files'
        wb_name = 'DataPipelineTestResults_'+d+'.xlsx'
        if wb_name not in os.listdir(wb_path):
            # create the excel workbook
            workbook = xlsxwriter.Workbook(os.path.join(wb_path, wb_name))
            worksheet = workbook.add_worksheet(name=file_ext.title())
            for i, method in enumerate(list(ParserFactory.current_parser_obj.__dict__.keys())):
                if str(ParserFactory.current_parser_obj.__dict__[method]).split(' ')[0].lstrip('<') == 'function' \
                        and '__' not in list(ParserFactory.current_parser_obj.__dict__.keys())[i]:
                        worksheet.write(0, i, list(ParserFactory.current_parser_obj.__dict__.keys())[i])
            workbook.close()

        else:
            pass












def main():
     parser_factory = ParserFactory()
     parser_factory.parse_file_ext(file_path=personal_umbrella, file_ext='docx')
     parser_factory.serialize_contents(write_path=pickle_path)
     #parser_factory.parser_results_xlsx(file_ext='doc')



if __name__ == '__main__':
    main()
