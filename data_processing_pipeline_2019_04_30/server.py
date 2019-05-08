"""
server
~~~~~~
"""
import os
import socket
import socks
import paramiko
from data_processing_pipeline_2019_04_30.configuration import (
    host, port, username, password, eml_write_path,
    rtf_write_path, docx_write_path, doc_write_path,
    pdf_write_path
)

# Sftp setup #
####################################################################################################
class SftpConnection(object):
    """Transfer data via sftp"""

    def __init__(self, root_path: str, remote_path: str):
        self.username = username
        self.password = password
        self.root_path = root_path
        self.remote_path = remote_path
        self.host = host
        self.port = int(port)

        self.sock: socks.socksocket = socks.socksocket()
        self.sftp: paramiko.Transport = None
        self.client: paramiko.SFTPClient.from_transport = None
        self.is_connected: bool = False
        self.sftp_connected: bool = False
        self.client_connected: bool = False


    def connect(self, filename: str, filepath: str):
        """connect to the remote server"""
        try:
            # set proxy connection values
            self.sock.set_proxy(
                proxy_type=None,
                addr=self.host,
                port=self.port,
                username=self.username,
                password=self.password
            )
            # connect the socket
            self.sock.connect((self.host, self.port))
            if socket.gethostname():
                self.is_connected = True
                print(f"Sftp connection to : {self.host} successful")
                # create transport
                self.sftp = paramiko.Transport(self.sock)
                try:
                    self.sftp.connect(
                        username=self.username,
                        password=self.password
                    )
                    # check if connection is live
                    if self.sftp.is_alive():
                        print("Transport is live")
                        self.sftp_connected = True

                        # create client and connect
                        try:
                            self.client = paramiko.SFTPClient.from_transport(self.sftp)
                            print(f"Client is: {self.client}")
                            self.client_connected = True
                            os.chdir(self.root_path)
                            self.transport_payload(filename=filename, filepath=filepath)
                            self.client.close()
                            print("closing client")

                            self.sftp.close()
                            print("closing sftp")

                            self.sock.close()
                            print("closing socket")

                        except:
                            print("A client error occurred")
                except:
                    print("An sftp error occurred")
        except:
            print("A socket error occurred")

    def transport_payload(self, filename: str, filepath: str):
        """Transport data to the remote server"""
        if self.sftp_connected and self.client_connected:
            try:
                while True:
                    os.chdir(filepath)
                    payload = filename
                    destination = self.remote_path + '/' + filename
                    self.client.put(
                        localpath=payload,
                        remotepath=destination
                    )
                    print(f"Successfully loaded {filename} to {destination}")
                    break
            except:
                print("An error occurred while transporting payload")
