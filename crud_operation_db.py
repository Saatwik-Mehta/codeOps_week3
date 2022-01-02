""" This module contains useful CRUD operation function that anyone
can use for their database table. It mainly required the nme of the DB and dataset."""

import logging
from typing import List

from mysql import connector

logging.basicConfig(filename='CRUD_operation.log',
                    level=logging.INFO,
                    format='%(asctime)s: %(levelname)s:'
                           ' %(filename)s->'
                           ' %(funcName)s->'
                           ' Line %(lineno)d-> %(message)s')

config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'saatwik',

}


def view_db_data(db_name: str = None, db_table: str = None, filter_data: dict = None):
    """

    This function shows you the data inside the database table.
    It provides you the functionality to filter the data also.

    :param filter_data: A dict variable which is used to
                        filter the data of the database table.

                        *-> Use keyname -> 'columns' to select the specific columns
                        from the db table.

                        Eg: filter_data = {'columns':'col1' or 'columns': 'col1,col2....'}

                        *-> Use keyname -> 'where' to use conditional
                        selection from the db table.
                        Eg: filter_data = {'where':'age>25' or
                        'where':'age>25 and/or salary<30000'}
    :param db_name: Database name which is containing the data table.
    :param db_table: Data Table name from which the data will be retrieved
    :return: string containing HTML table tag
    """
    try:
        if db_name is not None and db_table is not None:
            conn = connector.connect(**config)
            html_data = ''
            if conn.is_connected():
                cursor = conn.cursor()

                if filter_data is not None and isinstance(filter_data, dict):
                    if 'columns' in filter_data.keys() and filter_data['columns'] != '':
                        if 'where' in filter_data.keys():
                            cursor.execute(f'SELECT {filter_data["columns"]} FROM '
                                           f'{db_name}.{db_table} WHERE {filter_data["where"]}')
                        else:
                            cursor.execute(f'SELECT {filter_data["columns"]} '
                                           f'FROM {db_name}.{db_table}')
                    else:
                        if 'where' in filter_data.keys():
                            cursor.execute(f'SELECT * FROM {db_name}.{db_table} '
                                           f'WHERE {filter_data["where"]}')

                        else:
                            cursor.execute(f'SELECT * FROM {db_name}.{db_table}')
                else:
                    cursor.execute(f'SELECT * FROM {db_name}.{db_table}')
                columns = cursor.column_names
                db_data = cursor.fetchall()

                html_data += """<table><tr>"""
                for i in range(len(columns)):
                    html_data += f"""<th id="thead_{i}">{columns[i]}</th>"""
                html_data += """</tr>"""
                for rows in range(len(db_data)):
                    html_data += """<tr>"""
                    for item in range(len(db_data[rows])):
                        html_data += f"""<td id="td_{rows}.{item}">{db_data[rows][item]}</td>"""
                    html_data += """</tr>"""
                html_data += """</table>"""
            return html_data
        return f"Either DataBase {db_name} or Table {db_table} doesn't exist!"
    except connector.ProgrammingError as prog_err:
        logging.error('%s: %s', prog_err.__class__.__name__, prog_err)
        raise

    except connector.Error as err:
        logging.error('%s: %s', err.__class__.__name__, err)
        raise
    finally:
        conn.close()


def delete_db_data(db_name: str = None, db_table: str = None, expression=None):
    """
    This function can be used to delete a specific column from the database table.
    It requires the column name and value from the row which needs to be deleted.
    :param db_name: Database name which is containing the data table.
    :param db_table: Data Table name from which the data will be retrieved
    :param expression: A list variable that takes column name
                        and the value as string to be deleted.
                        Eg: expression=[column: str, value: str] ->
                            ['id','thi-id-123'] or ['id','2']
    :return: msg-> 'Deleted successfully' or error->"Programming error"
    """
    try:
        if db_name is not None and db_table is not None:
            conn = connector.connect(**config)
            if conn.is_connected():
                cursor = conn.cursor()
                if expression is not None \
                        and isinstance(expression, list) \
                        and len(expression) == 2:

                    if expression[1].isdigit():
                        cursor.execute(f'DELETE FROM {db_name}.{db_table} \
                        WHERE {expression[0]}={expression[1]}')
                    else:
                        cursor.execute(f'DELETE FROM {db_name}.{db_table} \
                        WHERE {expression[0]}="{expression[1]}"')
                    conn.commit()
                    return 'Deleted successfully'

                logging.warning('expression value is: %s', expression)
                raise TypeError(f'expression value is:{expression}')

            logging.warning("cannot make the connection with Mysql")
            return None
        return f"Either DataBase {db_name} or Table {db_table} doesn't exist!"
    except TypeError as type_err:
        logging.error('%s: %s', type_err.__class__.__name__, type_err)
        raise
    except connector.ProgrammingError as prog_err:
        logging.error('%s: %s', prog_err.__class__.__name__, prog_err)
        return "Programming error"
    except connector.Error as err:
        logging.error('%s: %s', err.__class__.__name__, err)
        return f"{err.__class__.__name__} error"
    finally:
        conn.close()


def update_db_data(db_name: str = None, db_table: str = None,
                   set_value: List[str] = None, target_exp: List[str] = None):
    """
    This function can be used to update a specific column value of the dataset.
    It requires the set_value=[column name, updated value]
    and target_exp=[column name, value]

    :param db_name: Database name which is containing the data table.
    :param db_table: Data Table name from which the data will be retrieved
    :param set_value: Data to be updated inside the dataset
                        Eg:- set_value=["original_title", "an original title"]

    :param target_exp: column name with value where update needs to be done.
                        Eg:- target_exp=['id', 'id_4']
    :return: msg->"Update successful"
    """
    try:
        if db_name is not None and db_table is not None:
            conn = connector.connect(**config)
            if conn.is_connected():
                cursor = conn.cursor()
                if isinstance(set_value, list) \
                        and len(set_value) == 2 \
                        and isinstance(target_exp, list) \
                        and len(target_exp) == 2:

                    if set_value[1].isdigit() and target_exp[1].isdigit():
                        cursor.execute(f"UPDATE {db_name}.{db_table} SET "
                                       f"{set_value[0]}={set_value[1]} "
                                       f"WHERE {target_exp[0]}={target_exp[1]}")

                    elif set_value[1].isdigit():
                        cursor.execute(f'UPDATE {db_name}.{db_table} SET '
                                       f'{set_value[0]}={set_value[1]} '
                                       f'WHERE {target_exp[0]}="{target_exp[1]}"')

                    elif target_exp[1].isdigit():
                        cursor.execute(f'UPDATE {db_name}.{db_table} SET '
                                       f'{set_value[0]}="{set_value[1]}" '
                                       f'WHERE {target_exp[0]}={target_exp[1]}')

                    else:
                        cursor.execute(f'UPDATE {db_name}.{db_table} SET '
                                       f'{set_value[0]}="{set_value[1]}" '
                                       f'WHERE {target_exp[0]}="{target_exp[1]}"')
                    conn.commit()
                    return "Update successful"

                logging.warning("set_value:%s target_exp:%s", set_value, target_exp)
                return None
            return None
        return None
    except connector.errors.ProgrammingError as prog_err:
        logging.error('%s: %s', prog_err.__class__.__name__, prog_err)
        raise
    except connector.Error as err:
        logging.error('%s: %s', err.__class__.__name__, err)
        raise
    finally:
        conn.close()


def create_db_data(db_name: str = None, db_table: str = None, row_values: dict = None):
    """
    This function can be used to Insert new row inside the database table.
    It takes the dict of column-value where column is from the data table
    and values that needs to be updated
    :param db_name: Database name which is containing the data table.
    :param db_table: Data Table name from which the data will be retrieved
    :param row_values: Takes the dict of column(key)-value(value)
                        pair to be stored in the datatable.
    :return: msg->"Successfully inserted"
    """

    try:
        if db_name is not None and \
                db_table is not None and \
                row_values is not None:
            conn = connector.connect(**config)
            if conn.is_connected():
                cursor = conn.cursor(buffered=True)
                columns = list(row_values.keys())
                columns = ','.join(columns)

                values = [row_values[col] for col in row_values]
                cursor.execute(f'INSERT INTO {db_name}.{db_table} '
                               f'({columns}) VALUES {tuple(values)}')
                conn.commit()
                conn.close()
                return "Successfully inserted"
            logging.warning("cannot make the connection with Mysql")
            return None
        return f"Either DataBase {db_name} or Table {db_table} doesn't exist!"
    except connector.ProgrammingError as prog_err:
        logging.error('%s: %s', prog_err.__class__.__name__, prog_err)
        return f"{prog_err.__class__.__name__}"
    except connector.IntegrityError as intg_err:
        logging.error('%s: %s', intg_err.__class__.__name__, intg_err)
        return "Integrity error"
    except connector.DatabaseError as db_err:
        logging.error('%s: %s', db_err.__class__.__name__, db_err)
        return "Database error"
    except connector.Error as err:
        logging.error('%s: %s', err.__class__.__name__, err)
        return f"{err.__class__.__name__} error"
    except AttributeError as att_err:
        logging.error('%s: %s', att_err.__class__.__name__, att_err)
        return f"{att_err.__class__.__name__}"
