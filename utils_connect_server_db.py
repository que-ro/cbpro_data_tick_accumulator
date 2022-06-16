import pymysql
import logging
import sshtunnel
from sshtunnel import SSHTunnelForwarder

class UtilsConnexion:

    ssh_host = 'xxx.xx.xxx.xx'
    ssh_username = 'xxx'
    ssh_password = 'xxxxxx'
    database_username = 'xxxxx'
    database_password = 'xxxxxxxxxxxxxxxxx'
    database_name = 'xxxxx'
    localhost = 'localhost'

    @staticmethod
    def get_ssh_tunnel(verbose=False):
        """Return an opened SSH tunnel and connect using a username and password.

        :param verbose: Set to True to show logging
        :return tunnel: SSH tunnel connection
        """

        if verbose:
            sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG

        tunnel = SSHTunnelForwarder(
            (UtilsConnexion.ssh_host, 22),
            ssh_username=UtilsConnexion.ssh_username,
            ssh_password=UtilsConnexion.ssh_password,
            remote_bind_address=('localhost', 3306)
        )

        tunnel.start()

        return tunnel

    @staticmethod
    def close_ssh_tunnel(tunnel):
        """Closes the SSH tunnel connection.

        :param: tunnel: SSH tunnel object connected to server
        """

        tunnel.close

    @staticmethod
    def get_mysql_connected(tunnel):
        """Return connection to a MySQL server using the SSH tunnel connection

        :param: tunnel: SSH tunnel object connected to server

        :return connection: Global MySQL database connection
        """

        connection = pymysql.connect(
            host=UtilsConnexion.localhost,
            user=UtilsConnexion.database_username,
            passwd=UtilsConnexion.database_password,
            db=UtilsConnexion.database_name,
            port=tunnel.local_bind_port
        )

        return connection

    def mysql_disconnect(connection):
        """Closes the MySQL database connection.

        :param connection: pymysql connection object
        """

        connection.close()

