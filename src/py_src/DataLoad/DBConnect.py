import mysql.connector

'''

!python3 -m pip install mysql-connector-python-rf
!python3 -m pip install mysql-connector-python

Allow local infile loading must be TRUE to load csv files.

>>> mysql> SHOW GLOBAL VARIABLES LIKE '%infile%';
    +---------------+-------+
    | Variable_name | Value |
    +---------------+-------+
    | local_infile  | OFF   |
    +---------------+-------+
    1 row in set (0.00 sec)

>>> mysql> SET GLOBAL local_infile=1;
    Query OK, 0 rows affected (0.00 sec)

>>> mysql> SHOW GLOBAL VARIABLES LIKE '%infile%';
    +---------------+-------+
    | Variable_name | Value |
    +---------------+-------+
    | local_infile  | ON    |
    +---------------+-------+
    1 row in set (0.00 sec)
'''


class DBConnect:
    def __init__(self, host):
        self.host = host

    def _mySqlConnect(self):
        _mySqlDB = mysql.connector.connect(
            host=self.host,  # "localhost",
            user="covidAnalyst",
            passwd="P@ssw0rd",
            auth_plugin='mysql_native_password',
            database="covid_data",
            allow_local_infile=True
        )
        return _mySqlDB
