"""
This module contains the HTTPSERVER(for python) for
uploading CSV only files and also uses csv_db.py module
(for creating generic db table)
and crud_operation_db module(for applying CRUD operations)
"""
import os

from http.server \
    import HTTPServer, \
    BaseHTTPRequestHandler
import cgi
import logging
from mysql import connector
import jinja2

logging.basicConfig(filename='CRUD_operation.log',
                    level=logging.INFO,
                    format='%(asctime)s: %(levelname)s:'
                           ' %(filename)s->'
                           ' %(funcName)s->'
                           ' Line %(lineno)d-> %(message)s')

from csv_db import csv_to_db as file_db
from crud_operation_db \
    import view_db_data, \
    delete_db_data, \
    update_db_data, \
    create_db_data

default_file = 'uploadFile_csv.csv'


class HttpRequestToResponse(BaseHTTPRequestHandler):
    """
    This class containing methods such as do_GET() do_POST()
    that are specifically useful for local server testing.

     Attributes
     ------------------------------
     FILE = name of the file user wats to use to save the data into.
            default set to uploadFile_csv.csv
    filter_data = A dict parameter used in do_POST() method for
                 filtering the table data when required.
    """
    global default_file
    filter_data = {}
    return_msg = None

    def do_GET(self):
        """The function to interact with db and file
        uploading functionality on client side browser."""
        try:
            if self.path.endswith('/'):
                self.send_response(200, message="this is message")
                self.send_header('content-type', 'text/html')
                self.end_headers()
                with open('Templates/index.html', 'r', encoding="utf-8") as index_html:
                    output = index_html.read()
                self.wfile.write(output.encode())
            if self.path.endswith('/upload'):
                self.send_response(200)
                self.send_header('content-type', 'text/html')
                self.end_headers()

                with open('Templates/uploadfile.html', 'r', encoding="utf-8") as upload_file:
                    output = upload_file.read()
                self.wfile.write(output.encode())

            if self.path.endswith('/view'):
                self.send_response(200)
                self.send_header('content-type', 'text/html')
                self.end_headers()
                table_name = os.path.splitext(default_file)[0].lower()
                table = view_db_data('fileupload', table_name,
                                     filter_data=self.filter_data)

                if table is not None:
                    with open('Templates/update_and_delete_row.html', 'r', encoding='utf-8')\
                            as view_file:
                        output = view_file.read()
                    render_output = jinja2.Template(output)
                    self.wfile.write((render_output.render(table=table).encode()))

            if self.path.endswith('/add'):
                self.send_response(200)
                self.send_header('content-type', 'text/html')
                self.end_headers()
                table_name = os.path.splitext(default_file)[0].lower()
                conn = connector.connect(host='localhost',
                                         user='root',
                                         password='saatwik')

                if conn.is_connected():
                    cursor = conn.cursor()
                    cursor.execute(f'SELECT * FROM fileupload.{table_name}')
                    db_column = cursor.column_names
                    with open('Templates/create_new_row.html', 'r', encoding='utf-8')\
                            as create_row_file:
                        output = create_row_file.read()
                    render_output = jinja2.Template(output)
                self.wfile.write(render_output.render(db_column=db_column).encode())

        except connector.ProgrammingError as prog_err:
            logging.error("%s: %s", prog_err.__class__.__name__, prog_err)
            self.send_error(code=prog_err.errno, message=f'{prog_err}')
        except connector.Error as conn_err:
            logging.error("%s: %s", conn_err.__class__.__name__, conn_err)

    def do_POST(self):
        """The function to upload the data in the db and
        perform crud operation using client browser."""
        global default_file
        try:
            if self.path.endswith('/upload'):
                c_type, pdict = cgi.parse_header(self.headers.get('content-type'))
                pdict['boundary'] = bytes(pdict['boundary'], "utf-8")
                content_len = int(self.headers.get('Content-length'))
                pdict['CONTENT-LENGTH'] = content_len
                if c_type == 'multipart/form-data':
                    fields = cgi.parse_multipart(self.rfile, pdict)
                    default_file = fields.get('your_filename')[0] \
                        if fields.get('your_filename')[0] != '' else default_file
                    filedata = fields.get('filedata')[0]
                    filedata = filedata.decode("utf-8-sig")
                    with open(default_file, mode="w", encoding="utf-8-sig") as newfile:
                        for data in filedata.split('\r\r'):
                            newfile.write(f"{data}")
                    if 'pk_field_datatype' in fields and 'primary_key' in fields:
                        file_db(filename=default_file,
                                pk_field_dtype=fields.get('pk_field_datatype')[0],
                                primary_key_field=fields.get('primary_key')[0])
                    else:
                        file_db(filename=default_file)
                self.send_response(301)
                self.send_header('content-type', 'text-html')
                self.send_header('Location', '/upload')
                self.end_headers()

            if self.path.endswith('/view'):
                table_name = os.path.splitext(default_file)[0].lower()
                c_type, pdict = cgi.parse_header(self.headers.get('content-type'))
                pdict['boundary'] = bytes(pdict['boundary'], "utf-8")
                content_len = int(self.headers.get('Content-length'))
                pdict['CONTENT-LENGTH'] = content_len
                if c_type == 'multipart/form-data':
                    fields = cgi.parse_multipart(self.rfile, pdict)
                    if 'textfield' in fields.keys():
                        columns = fields.get('textfield')[0]
                        self.filter_data['columns'] = columns
                    if 'expression_field' in fields.keys():
                        expression = fields.get('expression_field')[0]
                        expression = expression.split('=')
                        self.return_msg = delete_db_data('fileupload', table_name,
                                                         expression=expression)
                        # if isinstance(self.RETURN_MSG, tuple):
                        #     self.send_error(self.RETURN_MSG[0], message=self.RETURN_MSG[1])
                    if 'set_field' in fields.keys() and 'target_field' in fields.keys():
                        set_value = fields.get('set_field')[0]
                        set_value = set_value.split('=')
                        target_value = fields.get('target_field')[0]
                        target_value = target_value.split('=')
                        update_db_data('fileupload', table_name,
                                       set_value, target_value)
                self.send_response(301)
                self.send_header('content-type', 'text-html')
                self.send_header('Location', '/view')
                self.end_headers()

            if self.path.endswith('/add'):
                table_name = os.path.splitext(default_file)[0].lower()
                c_type, pdict = cgi.parse_header(self.headers.get('content-type'))
                pdict['boundary'] = bytes(pdict['boundary'], "utf-8")
                content_len = int(self.headers.get('Content-length'))
                pdict['CONTENT-LENGTH'] = content_len
                if c_type == 'multipart/form-data':
                    fields = cgi.parse_multipart(self.rfile, pdict)
                    fields = {key: fields[key][0] for key in fields}
                    row_values = fields
                    create_db_data('fileupload', table_name, row_values=row_values)

                self.send_response(301)
                self.send_header('content-type', 'text-html')
                self.send_header('Location', '/view')
                self.end_headers()
        except IndexError as ind_err:
            logging.error('%s: %s', ind_err.__class__.__name__, ind_err)
            self.send_error(code=406, message=f'{ind_err}')
        except PermissionError as perm_err:
            logging.error('%s:%s', perm_err.__class__.__name__, perm_err)
        except TypeError as type_err:
            logging.error('%s: %s',
                          type_err.__class__.__name__, type_err)
        except connector.ProgrammingError as prog_err:
            logging.error('%s: %s',
                          prog_err.__class__.__name__, prog_err)
            self.send_response(301)
            self.send_header('content-type', 'text-html')
            self.send_header('Location', '/view')
            self.end_headers()


def main():
    """Function to start the HTTP server using python script"""
    port = 8000
    # HttpRequestToResponse.FILE = 'museum_CSV.csv'
    server = HTTPServer(('localhost', port), HttpRequestToResponse)
    print("Server started on localhost: ", port)
    server.serve_forever()


if __name__ == "__main__":
    main()
