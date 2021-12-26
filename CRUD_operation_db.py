import logging
from mysql import connector

logging.basicConfig(filename='CRUD_operation.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%('
                                                                              'message)s')


def view_db_data(db_name: str = None, db_table: str = None, filter_data: dict = None):
    """
    This function shows you the data inside the database table.
    It provides you the functionality to filter the data also.

    :param filter_data: A dict variable which is used to filter the data of the database table.

                        *-> Use keyname -> 'columns' to select the specific columns from the db table.
                        Eg: filter_data = {'columns':'col1' or 'columns': 'col1,col2....'}

                        *-> Use keyname -> 'where' to use conditional selection from the db table.
                        Eg: filter_data = {'where':'age>25' or 'where':'age>25 and/or salary<30000'}
    :param db_name: Database name which is containing the data table.
    :param db_table: Data Table name from which the data will be retrieved
    :return: string containing HTML table tag
    """
    try:
        if db_name is not None and db_table is not None:
            conn = connector.connect(host='localhost',
                                     user='root',
                                     password='saatwik')
            html_data = ''
            if conn.is_connected():
                cursor = conn.cursor()

                if filter_data is not None and isinstance(filter_data, dict):
                    if 'columns' in filter_data.keys() and filter_data['columns'] != '':
                        if 'where' in filter_data.keys():
                            cursor.execute('SELECT {} FROM {}.{} WHERE {}'
                                           .format(filter_data['columns']
                                                   , db_name, db_table, filter_data['where']))
                        else:
                            cursor.execute('SELECT {} FROM {}.{}'
                                           .format(filter_data['columns']
                                                   , db_name, db_table))
                    else:
                        if 'where' in filter_data.keys():
                            cursor.execute('SELECT * FROM {}.{} WHERE {}'
                                           .format(db_name, db_table, filter_data['where']))
                        else:
                            cursor.execute('SELECT * FROM {}.{}'.format(db_name, db_table))
                else:
                    cursor.execute('SELECT * FROM {}.{}'.format(db_name, db_table))
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
        return prog_err.errno, prog_err.msg
    except connector.Error as err:
        logging.error('%s: %s', err.__class__.__name__, err)
    finally:
        conn.close()


def delete_db_data(db_name: str = None, db_table: str = None, expression=None):
    """

    :param db_name:
    :param db_table:
    :param expression:
    :return:
    """
    try:
        if db_name is not None and db_table is not None:
            conn = connector.connect(host='localhost',
                                     user='root',
                                     password='saatwik')
            if conn.is_connected():
                cursor = conn.cursor()
                if expression is not None and len(expression):

                    if expression[1].isdigit():
                        cursor.execute(f'DELETE FROM {db_name}.{db_table} WHERE {expression[0]}={expression[1]}')
                    else:
                        cursor.execute(f'DELETE FROM {db_name}.{db_table} WHERE {expression[0]}="{expression[1]}"')
                    conn.commit()
                    return 'Deleted successfully'
                else:
                    logging.warning('expression value is: %s', expression)
            else:
                logging.warning("cannot make the connection with Mysql")
    except connector.ProgrammingError as prog_err:
        logging.error('%s: %s', prog_err.__class__.__name__, prog_err)
        return prog_err.errno, prog_err.msg
    except connector.Error as err:
        logging.error('%s: %s', err.__class__.__name__, err)
    finally:
        conn.close()


def update_db_data(db_name: str = None, db_table: str = None, set_value=None, target_exp=None):
    """

    :param db_name:
    :param db_table:
    :param set_value:
    :param target_exp:
    :return:
    """
    try:
        if db_name is not None and db_table is not None:
            conn = connector.connect(host='localhost',
                                     user='root',
                                     password='saatwik')
            if conn.is_connected():
                cursor = conn.cursor()
                if set_value is not None and len(set_value) \
                        and target_exp is not None and len(target_exp):
                    if set_value[1].isdigit() and target_exp[1].isdigit():
                        cursor.execute(f"UPDATE {db_name}.{db_table} SET {set_value[0]}={set_value[1]} "
                                       f"WHERE {target_exp[0]}={target_exp[1]}")

                    elif set_value[1].isdigit():
                        cursor.execute(f'UPDATE {db_name}.{db_table} SET {set_value[0]}={set_value[1]} '
                                       f'WHERE {target_exp[0]}="{target_exp[1]}"')

                    elif target_exp[1].isdigit():
                        cursor.execute(f'UPDATE {db_name}.{db_table} SET {set_value[0]}="{set_value[1]}" '
                                       f'WHERE {target_exp[0]}={target_exp[1]}')

                    else:
                        cursor.execute(f'UPDATE {db_name}.{db_table} SET {set_value[0]}="{set_value[1]}" '
                                       f'WHERE {target_exp[0]}="{target_exp[1]}"')
                    conn.commit()

    except connector.ProgrammingError as prog_err:
        logging.error('%s: %s', prog_err.__class__.__name__, prog_err)

    except connector.Error as err:
        logging.error('%s: %s', err.__class__.__name__, err)
    finally:
        conn.close()


def create_db_data(db_name: str = None, db_table: str = None, row_values: dict = None):
    """

    :param db_name:
    :param db_table:
    :param row_values:
    :return:
    """

    try:
        if db_name is not None and db_table is not None:
            conn = connector.connect(host='localhost',
                                     user='root',
                                     password='saatwik')
            if conn.is_connected():
                cursor = conn.cursor(buffered=True)
                columns = [col for col in row_values.keys()]
                columns = ','.join(columns)

                values = [row_values[col] for col in row_values]
                cursor.execute(f'INSERT INTO {db_name}.{db_table} ({columns}) VALUES {tuple(values)}')
                conn.commit()
    except connector.ProgrammingError as prog_err:
        logging.error('%s: %s', prog_err.__class__.__name__, prog_err)
    except connector.Error as err:
        logging.error('%s: %s', err.__class__.__name__, err)
    finally:
        conn.close()
