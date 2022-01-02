""" This module contains the unit testing of CRUD operation functions.
 It includes both negative test cases and positive test case"""
import mysql.connector

from crud_operation_db import view_db_data, create_db_data, update_db_data, delete_db_data
from FakeDB import My_DB
from myfakedata import Value1, Value2, Value3, Value4, Value6

EMPTY_LIST = []

WRONG_DB_NAME = 1234

MYSQL_DB = 'Fake_DB'

DB_TABLE = 'test_table'


class TestCRUD(My_DB):
    """ This class contains test cases for CRUD operation functions,
    it includes how each function will be handling the inputs given by user.
    Possible Test Cases (positive and negative) are implemented here.
    """

    def test_access_db_unsuccessful(self):
        """ Check if the Database name is in correct and table name is in correct """
        with self.mock_db_config:
            self.assertEqual(create_db_data(db_name=WRONG_DB_NAME,
                                            db_table='None', row_values=Value1), "ProgrammingError")
            self.assertEqual(create_db_data(db_name='None', db_table='None',
                                            row_values=Value1), "ProgrammingError")

    def test_insert_in_db_successful(self):
        """ Check if Inserting data in database table is successful """
        with self.mock_db_config:
            self.assertEqual(create_db_data(db_name=MYSQL_DB, db_table=DB_TABLE, row_values=Value1),
                             "Successfully inserted")

            self.assertEqual(create_db_data(db_name=MYSQL_DB, db_table=DB_TABLE, row_values=Value2),
                             "Successfully inserted")

            self.assertEqual(create_db_data(db_name=MYSQL_DB, db_table=DB_TABLE, row_values=Value4),
                             "Successfully inserted")

    def test_insert_in_db_unsuccessful(self):
        """ Check if Inserting data in database table is unsuccessful """
        with self.mock_db_config:
            self.assertEqual(create_db_data(db_name=MYSQL_DB, db_table=DB_TABLE, row_values=Value3),
                             "Integrity error")
            self.assertEqual(create_db_data(db_name=MYSQL_DB, db_table=DB_TABLE, row_values=Value6),
                             "ProgrammingError")
            self.assertEqual(create_db_data(db_name=MYSQL_DB
                                            , db_table=DB_TABLE, row_values=EMPTY_LIST),
                             "AttributeError")

    def test_delete_row_in_db_successful(self):
        """ Check if deleting rows in database is successful"""
        with self.mock_db_config:
            self.assertEqual(delete_db_data(db_name=MYSQL_DB, db_table=DB_TABLE,
                                            expression=['id', 'id_1']), "Deleted successfully")

            self.assertEqual(delete_db_data(db_name=MYSQL_DB, db_table=DB_TABLE,
                                            expression=['rt_score', '80']), "Deleted successfully")

            self.assertEqual(delete_db_data(db_name=MYSQL_DB, db_table=DB_TABLE,
                                            expression=['vehicles', 'car']), "Deleted successfully")

    def test_delete_row_in_db_unsuccessful(self):
        """ Check if deleting rows in database is successful """

        self.assertEqual(delete_db_data(db_name=MYSQL_DB, db_table=DB_TABLE,
                                        expression=['ids', 'id_5']), "Programming error")
        self.assertEqual(delete_db_data(db_name=MYSQL_DB, db_table=DB_TABLE,
                                        expression=['id', '15']), "DataError error")
        with self.assertRaises(TypeError):
            delete_db_data(db_name=MYSQL_DB, db_table=DB_TABLE, expression=123)
        with self.assertRaises(TypeError):
            delete_db_data(db_name=MYSQL_DB, db_table=DB_TABLE, expression='')
        with self.assertRaises(TypeError):
            delete_db_data(db_name=MYSQL_DB, db_table=DB_TABLE, expression=[3356])
        with self.assertRaises(TypeError):
            delete_db_data(db_name=MYSQL_DB, db_table=DB_TABLE,
                           expression=[{'setgwett': 'asddgf vqeet'}])

    def test_read_from_db_successfully_finds_rows(self):
        """ Checking if Read operation for
         the Database is successful - the data being looked is successfully returned """
        with self.mock_db_config:
            self.assertIsNotNone(view_db_data(db_name=MYSQL_DB, db_table=DB_TABLE))

            self.assertIsNotNone(view_db_data(db_name=MYSQL_DB, db_table=DB_TABLE,
                                              filter_data={'columns': 'id,title,'
                                                                      'original_title'}))
            self.assertIsNotNone(view_db_data(db_name=MYSQL_DB, db_table=DB_TABLE,
                                              filter_data={'columns': 'id,title,'
                                                                      'original_title',
                                                           'where': 'id="id_2"'}))

    def test_read_from_db_unsuccessful(self):
        """ Checking if Read operation for the Database is unsuccessful """
        with self.assertRaises(mysql.connector.ProgrammingError):
            self.assertIsNotNone(view_db_data(db_name='Wrong database', db_table='wrong tanle'))

        with self.assertRaises(mysql.connector.ProgrammingError):
            self.assertIsNotNone(view_db_data(db_name=MYSQL_DB, db_table=DB_TABLE,
                                              filter_data={'columns': 'id,tititit,original_title'}))

        with self.assertRaises(mysql.connector.ProgrammingError):
            self.assertIsNotNone(view_db_data(db_name=MYSQL_DB, db_table=DB_TABLE,
                                              filter_data={'columns': 'id,title,original_title',
                                                           'where': 'id=e3e'}))

    def test_update_in_db_successful(self):
        """ Check if Update operation for the Database is successful -
         Data is updated successfully"""
        with self.mock_db_config:
            self.assertEqual(update_db_data(db_name=MYSQL_DB, db_table=DB_TABLE,
                                            set_value=["original_title", "an original title"],
                                            target_exp=['id', 'id_4']),
                             "Update successful")

            self.assertEqual(update_db_data(db_name=MYSQL_DB, db_table=DB_TABLE,
                                            set_value=["original_title", "an original title"],
                                            target_exp=['movie_banner', 'banner1.jpg']),
                             "Update successful")

    def test_update_in_db_unsuccessful(self):
        """ Checking if Update operation for the Database is unsuccessful -
                 data is not updated"""

        self.assertIsNone(update_db_data(db_name=MYSQL_DB, db_table=DB_TABLE))
        self.assertEqual(update_db_data(db_name=MYSQL_DB, db_table=DB_TABLE,
                                        set_value=["original_title", "an original title"]),
                         None)
        self.assertEqual(update_db_data(db_name=MYSQL_DB, db_table=DB_TABLE,
                                        set_value=[], target_exp=[]),
                         None)
        self.assertIsNone(update_db_data(db_name=MYSQL_DB, db_table=DB_TABLE,
                                         set_value=34567, target_exp=576547))

        with self.assertRaises(mysql.connector.IntegrityError):
            update_db_data(db_name=MYSQL_DB, db_table=DB_TABLE,
                           set_value=['id', 'id_2'],
                           target_exp=['title', 'iuwzdffbgrruy'])
