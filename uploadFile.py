from http.server import HTTPServer, BaseHTTPRequestHandler
import cgi
import logging
from csv_db import csv_to_db as file_db
from CRUD_operation_db import view_db_data

logging.basicConfig(filename='CRUD_operation.log', level=logging.INFO)


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
    FILE = 'uploadFile_csv.csv'
    filter_data = {}

    def do_GET(self):
        if self.path.endswith('/'):
            self.send_response(200)
            self.send_header('content-type', 'text/html')
            self.end_headers()
            output = """<!doctype>
                        <html>
                        <body>
                        <p style="font-weight:bold">Welcome to the Local Server!</p>
                        <a href = "/upload"> Upload file here </a>
                        </body>
                        </html>
            """
            self.wfile.write(output.encode())
        if self.path.endswith('/upload'):

            self.send_response(200)
            self.send_header('content-type', 'text/html')
            self.end_headers()

            output = ''
            output += '<!doctype html><html><body>'
            output += '<h3>upload your .csv file!</h3>'
            output += '<form method="post" name="myForm" onsubmit="return validateForm()" \
                        enctype="multipart/form-data" action = "/upload"> \
                        <input type="file" id="myFile" name="filename">\
                        <input type="submit"></form>' \
                      '<a href = "/view" >View your uploaded data</a>' \
                      '</body>'
            output += """
                                <script>
                        function validateForm() {
                          var x = document.forms["myForm"]["filename"].value;
                          var allowedExtensions = /(\\.csv)$/i;
                          if (x == "" || x == null) {
                            alert("Name must be filled out");
                            return false;
                          }
                          if (!x.match(allowedExtensions)){
                          alert('Please upload file having extension .csv only.');
                                document.forms["myForm"]["filename"].value='';
                                return false;
                                }
                         }
                        </script>
            """
            output += '</html>'

            self.wfile.write(output.encode())

        elif self.path.endswith('/view'):
            self.send_response(200)
            self.send_header('content-type', 'text/html')
            self.end_headers()

            output = view_db_data('fileupload', 'uploadfile_csv', filter_data=self.filter_data)

            if output is not None:
                output += """
                       <form method="post" enctype="multipart/form-data" action = "/view">
                        Columns: <input type="text" placeholder="Enter Columns" name="textfield" id="mytext">
                        <input type="submit">
                        </form>
                        <a href="/upload">back to upload files</a>
                """
                self.wfile.write(output.encode("utf-8-sig"))

    def do_POST(self):
        try:
            if self.path.endswith('/upload'):
                c_type, pdict = cgi.parse_header(self.headers.get('content-type'))
                pdict['boundary'] = bytes(pdict['boundary'], "utf-8")
                content_len = int(self.headers.get('Content-length'))
                pdict['CONTENT-LENGTH'] = content_len
                if c_type == 'multipart/form-data':
                    fields = cgi.parse_multipart(self.rfile, pdict)
                    filename = fields.get('filename')[0]
                    filename = filename.decode("utf-8-sig")

                    with open(self.FILE, mode="w", encoding="utf-8-sig") as file:
                        for data in filename.split('\r\r'):
                            file.write("%s" % data)

                file_db(filename=self.FILE)
                self.send_response(301)
                self.send_header('content-type', 'text-html')
                self.send_header('Location', '/upload')
                self.end_headers()

            if self.path.endswith('/view'):
                c_type, pdict = cgi.parse_header(self.headers.get('content-type'))
                pdict['boundary'] = bytes(pdict['boundary'], "utf-8")
                content_len = int(self.headers.get('Content-length'))
                pdict['CONTENT-LENGTH'] = content_len
                if c_type == 'multipart/form-data':
                    fields = cgi.parse_multipart(self.rfile, pdict)
                    columns = fields.get('textfield')[0]
                    self.filter_data['columns'] = columns
                self.send_response(301)
                self.send_header('content-type', 'text-html')
                self.send_header('Location', '/view')
                self.end_headers()

        except PermissionError as perm_err:
            logging.error(f'{perm_err.__class__.__name__}:{perm_err}')
        except Exception as exc:
            logging.error(f'{exc.__class__.__name__}:{exc}')


def main():
    port = 8000
    # HttpRequestToResponse.FILE = 'myCSV.csv'
    server = HTTPServer(('localhost', port), HttpRequestToResponse)
    print("Server started on localhost: ", port)
    server.serve_forever()


if __name__ == "__main__":
    main()
