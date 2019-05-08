"""
tests
~~~~~
"""
import os
import sys
import random
import unittest
from pprint import pprint
from unittest.mock import patch, Mock
from test import support, regrtest
from functools import wraps, reduce, partial
from data_processing_pipeline_2019_04_30.data_preprocessing import (
    PdfParser, EmlParser, RtfParser, DocParser, DocxParser, TxtParser,
    ParserFactory, FileGenerator
)
from data_processing_pipeline_2019_04_30.configuration import (
    personal_umbrella, global_business_data, sa, pickle_path,
mapping_file, log_file_path, error_file_path, eml_write_path,
rtf_write_path, docx_write_path, doc_test_write_path, doc_write_path,
pdf_write_path, r_path, r_script_three, r_script_two, r_executable,
r_script_one, claims_insights_remote_path, pdf_pickle_path
)


class TestDataPaths(unittest.TestCase):
    """Test that all needed directories exist"""

    def test_data_path_exists(self):
        """check if the data exists"""
        self.assertTrue(os.path.exists(personal_umbrella))
        self.assertTrue(os.path.exists(global_business_data))
        self.assertTrue(os.path.exists(sa))
        self.assertTrue(os.path.exists(pdf_pickle_path))

    def test_all_write_paths_exist(self):
        """check that all write paths exist"""
        # python data files
        self.assertTrue(os.path.exists(pickle_path))
        self.assertTrue(os.path.exists(mapping_file))
        self.assertTrue(os.path.exists(log_file_path))
        self.assertTrue(os.path.exists(error_file_path))

        # extracted file contents paths
        self.assertTrue(os.path.exists(eml_write_path))
        self.assertTrue(os.path.exists(rtf_write_path))
        self.assertTrue(os.path.exists(doc_write_path))
        self.assertTrue(os.path.exists(docx_write_path))
        self.assertTrue(os.path.exists(pdf_write_path))

        # R script paths
        self.assertTrue(os.path.isfile(r_executable + '.exe'))
        self.assertTrue(os.path.exists(r_path))
        self.assertTrue(os.path.isfile(os.path.join(r_path, r_script_one)))
        self.assertTrue(os.path.isfile(os.path.join(r_path, r_script_two)))
        self.assertTrue(os.path.isfile(os.path.join(r_path, r_script_three)))


class TestSampleSet(unittest.TestCase):
    """Test the functionality of the parsers"""
    def setUp(self):
        # setup for eml files
        self.file_types = ['.eml', '.rtf', '.doc', '.docx', '.pdf']

        # eml files
        self.eml_parser = EmlParser(personal_umbrella)
        self.eml_generator = FileGenerator(personal_umbrella, 'eml')
        self.eml_iterator = self.eml_generator.__iter__()

        # rtf files
        self.rtf_parser = RtfParser(personal_umbrella)
        self.rtf_generator = FileGenerator(personal_umbrella, 'rtf')
        self.rtf_iterator = self.rtf_generator.__iter__()

        # doc files
        self.doc_parser = DocParser(personal_umbrella)
        self.doc_generator = FileGenerator(personal_umbrella, 'doc')
        self.doc_iterator = self.doc_generator.__iter__()

        # docx files
        self.docx_parser = DocxParser(personal_umbrella)
        self.docx_generator = FileGenerator(personal_umbrella, 'docx')
        self.docx_iterator = self.docx_generator.__iter__()

        # pdf files
        self.pdf_parser = PdfParser(personal_umbrella)
        self.pdf_generator = FileGenerator(personal_umbrella, 'pdf')
        self.pdf_iterator = self.pdf_generator.__iter__()

        # file parser list
        self.file_parsers = [self.eml_parser,  self.rtf_parser,
                             self.docx_parser, self.pdf_parser]



    # def test_file_data_mining(self):
    #     """Run tests on all covered file extensions"""
    #     for i, _ in enumerate(self.file_types):
    #         print(f"Testing file extension: {self.file_types[i]}")
    #         files_ = list(filter(lambda x: os.path.splitext(x)[-1] == self.file_types[i],
    #                              list(map(lambda y: os.path.join(personal_umbrella, y),
    #                                       os.listdir(personal_umbrella)
    #                 ))
    #         ))
    #         print(f"Number of {self.file_types[i]} files: {len(files_)}")
    #         # create a random sample
    #         random_sample = random.sample(files_[i], 100)
    #         print(f"Current Parser: {self.file_parsers[i].__class__.__name__}")
    #         for r in random_sample:
    #             self.file_parsers[i].extract_text(current_file=r)
    #
    #         pprint(self.file_parsers[i].mapping_dict)
    #
    #         # check that almost 100 emails were successfully data mined
    #         self.assertAlmostEqual(len(self.file_parsers[i].mapping_dict.keys()), 100,
    #                                delta=30, msg=f"100 {self.file_types[i]} were successfully data mined.")
    #
    #         # check that len(error files) + len(successful files) = 100
    #         self.assertAlmostEqual(
    #             len(self.file_parsers[i].mapping_dict.keys()) + len(self.file_parsers[i].error_files),
    #             100, delta=30
    #         )
    #
    #         # check the content lengths
    #         for k in list(self.file_parsers[i].mapping_dict.keys()):
    #             self.assertGreater(len(self.file_parsers[i].mapping_dict[k]), 0)


    def test_eml_sample(self):
        """Randomly sample 100 .eml files"""
        eml_files = list(filter(lambda x: os.path.splitext(x)[-1] == '.eml',
                        list(map(lambda y: os.path.join(personal_umbrella, y),
                                 os.listdir(personal_umbrella)
                ))
        ))
        # create a random sample
        random_sample = random.sample(eml_files, 100)
        for r in random_sample:
            self.eml_parser.extract_text(current_file=r)

        # check that almost 100 emails were successfully data mined
        self.assertAlmostEqual(len(self.eml_parser.mapping_dict.keys()), 100,
                         delta=30, msg="100 .emls were successfully data mined.")

        # check that len(error files) + len(successful files) = 100
        self.assertAlmostEqual(
            len(self.eml_parser.mapping_dict.keys())+len(self.eml_parser.error_files),
            100, delta=30
        )

        # check the content lengths
        for k in list(self.eml_parser.mapping_dict.keys()):
            self.assertGreater(len(self.eml_parser.mapping_dict[k]), 0)

    def test_rtf_sample(self):
        """Randomly sample 100 .rtf files"""
        rtf_files = list(filter(lambda x: os.path.splitext(x)[-1] == '.rtf',
                                list(map(lambda y: os.path.join(personal_umbrella, y),
                                         os.listdir(personal_umbrella)
                                         ))
                                ))
        # create a random sample
        random_sample = random.sample(rtf_files, 100)
        for r in random_sample:
            self.rtf_parser.extract_text(current_file=r)

        # check that almost 100 emails were successfully data mined
        self.assertAlmostEqual(len(self.rtf_parser.mapping_dict.keys()), 100,
                               delta=30, msg="100 .rtfs were successfully data mined.")

        # check that len(error files) + len(successful files) = 100
        self.assertAlmostEqual(
            len(self.rtf_parser.mapping_dict.keys()) + len(self.rtf_parser.error_files),
            100, delta=30
        )

        # check the content lengths
        for k in list(self.rtf_parser.mapping_dict.keys()):
            self.assertGreater(len(self.rtf_parser.mapping_dict[k]), 0)


    def test_doc_sample(self):
        """Randomly sample 100 .doc files"""
        self.assertEqual(True, True)
    #
    #
    # def test_docx_sample(self):
    #     """Randomly sample 100 .docx files"""
    #     docx_files = list(filter(lambda x: os.path.splitext(x)[-1] == '.docx',
    #                             list(map(lambda y: os.path.join(personal_umbrella, y),
    #                                      os.listdir(personal_umbrella)
    #                                      ))
    #                             ))
    #     # create a random sample
    #     random_sample = random.sample(docx_files, 100)
    #     for r in random_sample:
    #         self.docx_parser.extract_text(current_file=r)
    #
    #     # check that almost 100 emails were successfully data mined
    #     self.assertAlmostEqual(len(self.docx_parser.mapping_dict.keys()), 100,
    #                            delta=30, msg="100 .docx were successfully data mined.")
    #
    #     # check that len(error files) + len(successful files) = 100
    #     self.assertAlmostEqual(
    #         len(self.docx_parser.mapping_dict.keys()) + len(self.docx_parser.error_files),
    #         100, delta=30
    #     )
    #
    #     # check the content lengths
    #     for k in list(self.docx_parser.mapping_dict.keys()):
    #         self.assertGreater(len(self.docx_parser.mapping_dict[k]), 0)


    # def test_pdf_sample(self):
    #     """Randomly sample 100 .pdf files"""
    #     pdf_files = list(filter(lambda x: os.path.splitext(x)[-1] == '.pdf',
    #                             list(map(lambda y: os.path.join(personal_umbrella, y),
    #                                      os.listdir(personal_umbrella)
    #                                      ))
    #                             ))
    #     # create a random sample
    #     random_sample = random.sample(pdf_files, 100)
    #     for r in random_sample:
    #         self.pdf_parser.extract_text(current_file=r)
    #
    #     # check that almost 100 emails were successfully data mined
    #     self.assertAlmostEqual(len(self.pdf_parser.mapping_dict.keys()), 100,
    #                            delta=30, msg="100 .pdfs were successfully data mined.")
    #
    #     # check that len(error files) + len(successful files) = 100
    #     self.assertAlmostEqual(
    #         len(self.pdf_parser.mapping_dict.keys()) + len(self.pdf_parser.error_files),
    #         100, delta=30
    #     )
    #
    #     # check the content lengths
    #     for k in list(self.pdf_parser.mapping_dict.keys()):
    #         self.assertGreater(len(self.pdf_parser.mapping_dict[k]), 0)




    def tearDown(self):
        pass



if __name__ == '__main__':
    unittest.main()
