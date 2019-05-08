"""
pdf_ocr
~~~~~~~
Optical Character Recognition(OCR) for PDF text.

 {'File Name': f,
    f'Page_{n}':
{'Text': text, 'Images': [img]}}

# Data Structure #
[
    {'File Name': file.pdf,
     'Page_0': {'Text': 'text string', 'Image File 1': img.png, ...}
    }

]

"""
import io
import os
import abc
import time
import PyPDF2
import pytesseract
from PIL import Image
from wand.image import Image as wand_img
from data_processing_pipeline_2019_04_30.configuration import extracted_pdf_images

# path to tesseract executable
pytesseract.pytesseract.tesseract_cmd = 'C:\\Program Files\\Tesseract-OCR\\tesseract'





class OCRInterface(metaclass=abc.ABCMeta):
    """An abstract interface for optical character recognition"""

    @abc.abstractmethod
    def extract_pdf_images(self, file_path: str):
        """load in the current file"""
        pass

    @abc.abstractmethod
    def ocr_image_file(self, img_file_path: str):
        """ocr an image file"""
        pass

class PdfOCR(OCRInterface):
    """OCR pdf documents"""

    def __init__(self):
        self.extracted_pdfs = []
        self.pdf_contents   = []

    def extract_pdf_images(self, file_path: str):
        try:
            if os.path.exists(file_path):
                for f in os.listdir(file_path):
                    print(f'Current Pdf: {f}')
                    if os.path.splitext(f)[-1] == '.pdf':
                        # open the current pdf
                        pdf_reader = PyPDF2.PdfFileReader(
                            open(os.path.join(file_path, f), 'rb')
                        )
                        # get the number of pages
                        num_pages = pdf_reader.getNumPages()
                        # create a dictionary for the current pdf
                        current_pdf = {}
                        print(f'number of pages: {num_pages}')
                        # iterate through each page and extract the pdf's contents
                        n = 0
                        while n < num_pages:
                            # get the current page
                            page = pdf_reader.getPage(n)
                            # get the xObject
                            xObject = page['/Resources']['/XObject'].getObject()
                            # get any text that may exist on the page
                            text = page.extractText()
                            # sub page counter
                            m = 0

                            # iterate through the xObject's elements
                            for obj in xObject:
                                if xObject[obj]['/Subtype'] == '/Image':
                                    size = (xObject[obj]['/Width'], xObject[obj]['/Height'])
                                    data = xObject[obj]._data
                                    if xObject[obj]['/ColorSpace'] == '/DeviceRGB':
                                        mode = "RGB"
                                    else:
                                        mode = "P"

                                if xObject[obj]['/Filter'] == '/FlateDecode':
                                    # .png
                                    # create a directory for the image
                                    pdf_name = os.path.basename(os.path.splitext(f)[0])  # current pdf

                                    if not os.path.exists(os.path.join(extracted_pdf_images, pdf_name)):
                                        # create a directory for the current pdf
                                        new_dir = os.path.join(extracted_pdf_images, pdf_name)
                                        os.mkdir(new_dir)
                                        time.sleep(4)

                                    # save the image file
                                    img = Image.frombytes(mode, size, data)
                                    img.save(os.path.join(new_dir, f'ImgFilePage{n}_{m}.png'))
                                    m += 1

                                elif xObject[obj]['/Filter'] == '/DCTDecode':
                                    # .jpg
                                    # create a directory for the image
                                    pdf_name = os.path.basename(os.path.splitext(f)[0])  # current pdf

                                    if not os.path.exists(os.path.join(extracted_pdf_images, pdf_name)):
                                        # create a directory for the current pdf
                                        new_dir = os.path.join(extracted_pdf_images, pdf_name)
                                        os.mkdir(new_dir)
                                        time.sleep(4)

                                    # save the image file
                                    img = open(os.path.join(new_dir, f'ImgFilePage{n}_{m}.jpg'), "wb")
                                    img.write(data)
                                    img.close()
                                    m += 1

                                elif xObject[obj]['/Filter'] == '/JPXDecode':
                                    # .jp2
                                    # create a directory for the image
                                    pdf_name = os.path.basename(os.path.splitext(f)[0])  # current pdf

                                    if not os.path.exists(os.path.join(extracted_pdf_images, pdf_name)):
                                        # create a directory for the current pdf
                                        new_dir = os.path.join(extracted_pdf_images, pdf_name)
                                        os.mkdir(new_dir)
                                        time.sleep(4)

                                    # save the image file
                                    img = open(os.path.join(new_dir,  f'ImgFilePage{n}_{m}.jp2'), "wb")
                                    img.write(data)
                                    img.close()
                                    m += 1
                                else:
                                    print(f'File: {f}: No images on page: {n}')
                            # increment the page counter
                            n += 1
        except:
            print('An error has occurred')


    def ocr_image_file(self, img_file_path: str):
        try:
            if os.path.isdir(img_file_path):
                for f in os.listdir(img_file_path):
                    print(f'Extracting text from file: {f}')
                    try:
                        img_ = wand_img(filename=os.path.join(img_file_path, f), resolution=300)
                        wnd_img = wand_img(image=img_).make_blob(
                            os.path.splitext(f)[-1].lstrip('.')
                        )
                        im = Image.open(io.BytesIO(wnd_img))
                        text = pytesseract.image_to_string(im, lang='eng')
                        self.extracted_pdfs.append(text)
                    except:
                        print(f'An error occurred while ocring image: {f}')
            else:
                raise OSError(f'OSError: Path, {img_file_path} not found.')
        except OSError as e:
            print(e)


img_file = "Y:\\Shared\\USD\\Business Data and Analytics\\Claims_Pipeline_Files" \
           "\\BDA_Cliams_Pipeline\\extracted_pdf_images" \
           "\\Neutral_381977_Allstate Policy_All_Images_withText_All_Images_Text" \
           "\\ImgFilePage1_0.jpg"


def ocr_image(file_name: str):
    """Ocr an image"""
    img_ = wand_img(filename=file_name, resolution=300)
    wnd_img = wand_img(image=img_).make_blob("jpg")
    im = Image.open(io.BytesIO(wnd_img))
    text = pytesseract.image_to_string(im, lang='eng')
    print(text)









def main():
    neutral_pdfs_path = "Y:\\Shared\\USD\\Business Data and Analytics" \
                        "\\Claims_Pipeline_Files\\Sample_Pdfs\\Neutral"

    image_files = "Y:\\Shared\\USD\\Business Data and Analytics\\Claims_Pipeline_Files\\BDA_Cliams_Pipeline\\" \
                  "extracted_pdf_images\\Neutral_381977_Allstate Policy_All_Images_withText_All_Images_Text"


    pdf_one = os.path.join(neutral_pdfs_path,
                "Neutral_381977_Allstate Policy_All_Images_withText_All_Images_Text.pdf")

    pdf_ocr = PdfOCR()
    pdf_ocr.ocr_image_file(img_file_path=image_files)

    #pdf_ocr.extract_pdf_contents(file_path=neutral_pdfs_path)


if __name__ == '__main__':
    main()
