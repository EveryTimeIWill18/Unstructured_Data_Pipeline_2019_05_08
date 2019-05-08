"""
hive
~~~~
"""
import os
import socket
import paramiko
from data_processing_pipeline_2019_04_30.configuration import (
    username, password, host, port
)


# SSHConnection(Parent Class) #
####################################################################################################
class SSHConnection(object):
    """Creates an ssh connection"""
    def __init__(self, username: str, password: str, host: str, port: int):
        self.username = username
        self.password = password
        self.host = host
        self.port = int(port)
        self.ssh = paramiko.SSHClient()
        self.connected = False

    def connect(self):
        """connect to a remote server via ssh"""
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy)
        self.ssh.connect(
            hostname=self.host,
            username=self.username,
            password=self.password
        )
        if socket.gethostname():
            self.connected = True
            print(socket.gethostname())
            print(socket.getfqdn())
            print(socket.gethostbyaddr(self.host))

# Child Classes #
####################################################################################################
class HivCli(SSHConnection):
    """Connect to Hive command line interface."""

    def __init__(self, username: str, password: str, host: str, port: int):
        super().__init__(username, password, host, port)
        self.host: str = host
        self.port: int = int(port)
        self.username: str = username
        self.password: str = password
        self.connect()  # connect to the remote server

    def build_hive_query(self, database: str, *args, **kwargs):
        """build the query that will be submitted to Hive cli."""
        cmd_start = f'hive -e "use {database}; '
        cmd_end = ';"'


class Hive(SSHConnection):
    """Connect to Hive remotely"""

    def __init__(self, username: str, password: str, host: str, port: int):
        super().__init__(username, password, host, port)
        self.host: str = host
        self.port: int = int(port)
        self.username: str = username
        self.password: str = password
        self.query_string: str = "Hello"
        self.connect() # connect to the remote server

    def build_hive_query(self, query_method: str, table_name: str):
        """build and submit a hive query"""
        if query_method in Hive.__dict__:
            cmd_start = 'hive -e "use drw; '
            cmd_end = ';"'
            # run the desired function
            Hive.__dict__[query_method](self, table_name=table_name)

    def create_hive_table(self, *args, table_name: str, database: str, **kwargs):
        """create a hive table"""
        cmd_start = f'hive -e "use {database}; '
        cmd_end = ';"'
        table_values = ' '.join(k+ ' ' + kwargs[k] + ',' for k in kwargs).rstrip(', ')
        query_string = f"CREATE TABLE IF NOT EXISTS {table_name}({table_values})"\
            f"ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"

        # create the correct query string
        output = cmd_start + query_string + cmd_end
        print(output)

        if not self.connected:
            self.connect()

        # make hive query
        stdin_, stdout_, stderr_ = self.ssh.exec_command(output)
        stdout_.channel.recv_exit_status()
        lines = stdout_.readlines()
        # check query output
        for line in lines:
            print(line)

    def load_hive_table(self, file_path: str, file_name: str, table_name: str):
        """load data into the specified hive table"""
        cmd_start = 'hive -e "use drw; '
        cmd_end = ';"'
        query_string = f'LOAD DATA LOCAL INPATH "{os.path.join(file_path, file_name)}"\
        OVERWRITE INTO TABLE {table_name}'
        output = cmd_start + query_string + cmd_end

        if not self.connected:
            self.connect()

        # make hive query
        stdin_, stdout_, stderr_ = self.ssh.exec_command(output)
        stdout_.channel.recv_exit_status()
        lines = stdout_.readlines()
        # check query output
        for line in lines:
            print(line)

    def update_hive_table(self, table_name: str):
        """update specified hive table"""
        cmd_start = 'hive -e "use drw; '
        cmd_end = ';"'
        query_string = f'DROP TABLE IF EXISTS {table_name}'
        output = cmd_start + query_string + cmd_end

        if not self.connected:
            self.connect()

        # make hive query
        stdin_, stdout_, stderr_ = self.ssh.exec_command(output)
        stdout_.channel.recv_exit_status()
        lines = stdout_.readlines()
        # check query output
        for line in lines:
            print(line)


    def drop_hive_table(self, table_name: str):
        """remove specified hive table"""
        cmd_start = 'hive -e "use drw; '
        cmd_end = ';"'

        query_string = f'DROP TABLE IF EXISTS {table_name}'
        output = cmd_start + query_string + cmd_end

        if not self.connected:
            self.connect()

        # make hive query
        stdin_, stdout_, stderr_ = self.ssh.exec_command(output)
        stdout_.channel.recv_exit_status()
        lines = stdout_.readlines()
        # check query output
        for line in lines:
            print(line)



    def query_hive_table(self, *args, table_name: str):
        """query specified hive table"""
        cmd_start = 'hive -e "use drw; '
        cmd_end = ';"'
        hive_commands = ' '.join(a for a in args)
        output = cmd_start \
                 + hive_commands \
                 + cmd_end

        if not self.connected:
            self.connect()

        # make hive query
        stdin_, stdout_, stderr_ = self.ssh.exec_command(output)
        stdout_.channel.recv_exit_status()
        lines = stdout_.readlines()
        # check query output
        for line in lines:
            print(line)



class HDFS(SSHConnection):
    """Load data into HDFS"""

    def __init__(self, file_path: str, username: str, password: str, host: str, port: int, ):
        super().__init__(username, password, host, port)
        self.file_path = file_path
        self.host = host
        self.port = port
        self.username = username
        self.password = password

    def load_into_hdfs(self, hdfs_path: str, *files):
        """load files into hdfs"""
        try:
            if self.connected:
                data_to_load = [f for f in files]
                for data in data_to_load:
                    payload = self.file_path + '/' + data
                    hdfs_dir = hdfs_path + '/' + data
                    try:
                        stdin_, stdout_, stderr_ = self.ssh.exec_command(
                            f'hdfs dfs -put {payload} {hdfs_dir}'
                        )
                    except:
                        print(f"HDFSLoadError: Failed to load {data} into HDFS.")
            else:
                raise ConnectionError(f"Not connected to server: {self.host}")
        except ConnectionError:
            print("A connection error occurred.")
