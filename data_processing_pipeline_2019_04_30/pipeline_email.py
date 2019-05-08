"""
pipeline_email
~~~~~~~~~~~~~~

"""
import os
import abc
import sys
import base64
import html
import smtplib
from pprint import pprint
from datetime import datetime
from optparse import OptionParser
from jinja2 import Template, FileSystemLoader, Environment, PackageLoader
from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from itertools import chain
from functools import wraps, reduce, partial



# HTML Email Template #
####################################################################################################
class HTML_Template(object):
    """Load in an HTML Template"""

    def __init__(self, template_path: str, business_segment: str, html_file: str):
        self.template_path: str = template_path
        self.html_file = html_file
        self.business_segment: str = business_segment
        pprint(self.template_path)

        # jinja2 template variables
        self.loader = FileSystemLoader(self.template_path)
        self.environment: Environment = Environment(self.loader)
        self.template = None
        #self.html_output: str = None

    def load_template(self, **template_kwargs):
        """Load in the current template file"""
        self.template = self.environment.get_template(self.html_file)


        #self.html_output = self.template.render(template_kwargs)
        #pprint(self.html_output)



# Emailer Interface #
####################################################################################################
class PipelineEmailerInterface(metaclass=abc.ABCMeta):
    """An email interface for creating customized emails"""

    @abc.abstractproperty
    def host___(self):
        pass

    @abc.abstractmethod
    @host___.setter
    def host___(self, host: str):
        """host of the email server"""
        pass

    @abc.abstractproperty
    def to___(self):
        pass

    @abc.abstractmethod
    @to___.setter
    def to___(self, *recipients):
        """Created a dictionary of recipients of the email"""
        pass

    @abc.abstractproperty
    def from___(self):
        pass

    @abc.abstractmethod
    @from___.setter
    def from___(self, sender: str):
        """Sender of the email"""
        pass

    @abc.abstractproperty
    def subject___(self):
        pass

    @abc.abstractmethod
    @subject___.setter
    def subject___(self, sub: str):
        """Subject of the email"""
        pass

    @abc.abstractproperty
    def body___(self):
        pass

    @abc.abstractmethod
    @body___.setter
    def body___(self, b: str):
        """Body fo the email"""
        pass

    @abc.abstractproperty
    def attachments___(self):
        pass

    @abc.abstractmethod
    @attachments___.setter
    def attachments___(self, *atmnts):
        """Add attachments to the email"""
        pass

    @abc.abstractmethod
    def send_email(self):
        """Send an email"""

# Concrete Emailer Implementation #
####################################################################################################
class PipelineEmailer(PipelineEmailerInterface):
    """Concrete implementation of the email interface"""

    def __init__(self):
        self._host = None
        self._to: list = []
        self._from: str = None
        self._body: str = None
        self._subject: str = None
        self._attachments: str = None
        self.mail_server:smtplib.SMTP = None

    @property
    def host___(self):
        return self._host
    @host___.setter
    def host___(self, host: str):
        self._host = host

    @property
    def to___(self):
        return self._to
    @to___.setter
    def to___(self, *to):
       self._to = list(chain.from_iterable(list(to)))

    @property
    def from___(self):
        return self._from
    @from___.setter
    def from___(self, f: str):
        self._from = f

    @property
    def subject___(self):
        return self._subject
    @subject___.setter
    def subject___(self, subject: str):
        self._subject = subject

    @property
    def body___(self):
        return self._body
    @body___.setter
    def body___(self, body: str):
        self._body = body

    @property
    def attachments___(self):
        return self._attachments
    @attachments___.setter
    def attachments___(self, *attachments):
        pass

    def send_email(self):
        """Send an email"""
        try:
            # start the email server
            self.mail_server = smtplib.SMTP(self.host___)


            COMMASPACE = ', '
            # create the message
            message = MIMEText(self.body___, 'html')
            message['From'] = self.from___
            message['To'] = COMMASPACE.join(self.to___)
            message['Subject'] = self.subject___

            # send the email
            self.mail_server.sendmail(message['From'], message['To'], message.as_string())
        except:
            print("email error")
        finally:
            # close the message server
            self.mail_server.quit()





def main():

    # jinja2 template setup
    html_path = 'C:\\Users\\wmurphy\\PycharmProjects\\Data_Processing_Pipeline_2019_4_30\\templates'
    html_file = 'email_template.html'
    today = str(datetime.today())[:10].replace("-", "_")
    file_loader = FileSystemLoader(html_path)
    environment = Environment(loader=file_loader)
    template = environment.get_template(html_file)
    output = template.render(
        business_segment='NA PC Unstructured Claims Pipeline Results',
        hadoop_env='MSEAULXPA03',
        python_path=sys.executable.__str__(),
        csv_file='csv_file_path.csv',
        hadoop_path=f'/bda/claimsops/data/NA_PC_DMS_Claims_Delta/{today}',
        hive='na_pc_dms_claims',
        code_version='1.0',
        daily_win_total=99,
        dms_total=100,
        total_python=500,
        total_csv_file=500,
        total_hdfs=1000,
        total_hive=808,
        win_less=1200,
        total_py_pyerr=1187
    )
#  'prakriti.jha@genre.com', 'amy.mcnamara@genre.com', 'vamsi.sabbisetty@genre.com'

    pipeline_email = PipelineEmailer()
    pipeline_email.host___ = "mail.genre.com"
    pipeline_email.to___ = ['william.murphy@genre.com']
    pipeline_email.body___ = output
    pipeline_email.from___ = 'william.murphy@genre.com'
    pipeline_email.subject___ = 'Python Emailer Test' + str(datetime.now())
    pipeline_email.send_email()
if __name__ == '__main__':
    main()

