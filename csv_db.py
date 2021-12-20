import logging
import os.path
from mysql import connector
import pandas as pd

logging.basicConfig(filename='CRUD_operation.log', level=logging.INFO)

PYTHON_TO_SQL_TYPE = {'string': 'TEXT',
                      'floating': 'FLOAT',
                      'integer': 'INT',
                      'boolean': 'BOOLEAN',
                      'date': 'DATE',
                      'time': 'TIME'}


def csv_to_db(filename=None, primary_key_field: str = None, pk_field_dtype=None):
    """
    Function use to convert your csv file data into database table.
    Takes the CSV file as input and Primary field to set in the dataTable.

    :param primary_key_field: Name of the column in your csv
                              file that should be used as the primary key \
                              in the db table.
    :param pk_field_dtype: Set the data type of the field that should be used
                            as primary key.
    :param filename: Name of the file which contains the data to be stored
                    in the db table. The filename will also be used as the
                    name of the db table as well.
    :return: None
    """
    try:
        if filename is not None and \
                os.path.isfile(filename) and \
                os.path.splitext(filename)[-1] == '.csv':
            table_name = os.path.splitext(filename)[0].lower()
            csv_dataframe = pd.read_csv(filename, keep_default_na=False)
            column_inf_dtype = [pd.api.types.infer_dtype(csv_dataframe[i], skipna=True)
                                for i in csv_dataframe.columns]
            mysql_dtype_column = [PYTHON_TO_SQL_TYPE[i] for i in column_inf_dtype]
            column_with_dtype = [(i[0] + " " + i[1])
                                 for i in zip(csv_dataframe[csv_dataframe.columns]
                                              , mysql_dtype_column)]
            if primary_key_field is not None:
                for i in range(len(column_inf_dtype)):
                    if primary_key_field in column_inf_dtype[i]:
                        column_inf_dtype[i] = " NOT NULL PRIMARY KEY"
                        break

            conn = connector.connect(host='localhost',
                                     user='root',
                                     password='saatwik')

            if conn.is_connected():
                cursor = conn.cursor()
                cursor.execute("CREATE DATABASE IF NOT EXISTS fileupload")
                cursor.execute("USE fileupload")
                cursor.execute('DROP TABLE IF EXISTS {}'.format(table_name))
                cursor.execute('CREATE TABLE IF NOT EXISTS {} ({})'.
                               format(table_name, ','.join(column_with_dtype)))
                for i in csv_dataframe.values.tolist():
                    cursor.execute('INSERT INTO {} VALUES {}'.format(table_name, tuple(i)))
                conn.commit()
                conn.close()

    except connector.ProgrammingError as pro_err:
        logging.error("%s: %s", pro_err.__class__.__name__, pro_err)
    except connector.IntegrityError as integrity_err:
        logging.error("Error occured %s: %s",
                      integrity_err.__class__.__name__, integrity_err)
    except ValueError as val_err:
        logging.error("%s: %s", val_err.__class__.__name__, val_err)
    except Exception as exc:
        logging.error("%s: %s", exc.__class__.__name__, exc)
