import logging
from mysql import connector

logging.basicConfig(filename='CRUD_operation.log', level=logging.INFO)


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
                conn.close()

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
    except connector.Error as err:
        logging.error('%s: %s', err.__class__.__name__, err)

