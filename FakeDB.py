import sys
import unittest
from unittest.mock import patch
import mysql.connector
from mysql.connector import errorcode
import crud_operation_db

MYSQL_USER = "root"
MYSQL_PASSWORD = "saatwik"
MYSQL_DB = 'Fake_DB'
MYSQL_HOST = "localhost"


class My_DB(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        cnx = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,

        )
        cursor = cnx.cursor()

        # drop database if it already exists
        try:
            cursor.execute("DROP DATABASE {}".format(MYSQL_DB))
            cursor.close()
            print("DB dropped")
        except mysql.connector.Error as err:
            print("{}{}".format(MYSQL_DB, err))

        cursor = cnx.cursor()
        try:
            cursor.execute(
                "CREATE DATABASE {}".format(MYSQL_DB))
        except mysql.connector.Error as err:
            print("Failed creating database: {}".format(err))
            sys.exit(1)
        cnx.database = MYSQL_DB

        query = """CREATE TABLE test_table (
                  id varchar(50) PRIMARY KEY,
                  title text,
                  original_title text,
                  original_title_romanised text,
                  image text,
                  movie_banner text, 
                  description text,
                  director text,
                  producer text,
                  release_date int,
                  running_time int,
                  rt_score int,
                  people text,
                  species text,
                  locations text, 
                  vehicles text,
                  url text
                )"""
        try:
            cursor.execute(query)
            cnx.commit()
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("test_table already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

        insert_data_query = """INSERT INTO test_table VALUES
                            ('id_1','vroom',
                            'vroom_orginal','vroom_romanised',
                             'image.jpg','banner.jpg',
                             'it is a movie','ramaj','nuthan',
                             2021,100,99,'some,people,movie','human'
                             ,'anylocation','car,plane','aurl'),
                             ('id_2','grom',
                            'grom_orginal','grom_romanised',
                             'image1.jpg','banner1.jpg',
                             'it is another movie','ramanuj','nuthanil',
                             2001,90,99,'some,people,movie','human'
                             ,'anylocation','car,plane','aurl')
                            """
        try:
            cursor.execute(insert_data_query)
            cnx.commit()
        except mysql.connector.Error as err:
            print("Data insertion to test_table failed \n" + err)
        cursor.close()
        cnx.close()

        testconfig = {
            'host': MYSQL_HOST,
            'user': MYSQL_USER,
            'password': MYSQL_PASSWORD,

        }
        cls.mock_db_config = patch.dict(crud_operation_db.config, testconfig)

    @classmethod
    def tearDownClass(cls):

        cnx = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
        )
        cursor = cnx.cursor()

        # drop test database
        try:
            cursor.execute("DROP DATABASE {}".format(MYSQL_DB))
            cnx.commit()
            cursor.close()
        except mysql.connector.Error as err:
            print("Database {} does not exists. Dropping db failed".format(MYSQL_DB))
        cnx.close()
