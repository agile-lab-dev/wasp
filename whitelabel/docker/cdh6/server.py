#!/usr/bin/env python3
"""
Very simple HTTP server in python for logging requests
Usage::
    ./server.py [ssl]
"""
from http.server import BaseHTTPRequestHandler, HTTPServer
import logging
import ssl

class ServerHandler(BaseHTTPRequestHandler):
    def _set_response(self, code):
        self.send_response(code)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        response = '{"id": "0","text": "TestResponse"}'
        self.wfile.write(bytes(response, 'utf-8'))

    def do_GET(self):
        logging.info("GET request,\nPath: %s\nHeaders:\n%s\n", str(self.path), str(self.headers))
        self._set_response(200)

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        with open("written_data.txt", "a") as output_file:
            post_data = self.rfile.read(content_length).decode('utf-8')
            str_data = str(post_data + "\n")
            output_file.write(str_data)

        self._set_response(200)

def run(server_class=HTTPServer, handler_class=ServerHandler, port=4480, useSsl = False):
    logging.basicConfig(level=logging.INFO)
    server_address = ('0.0.0.0', port)
    httpd = server_class(server_address, handler_class)
    if (useSsl):
        httpd.socket = ssl.wrap_socket(httpd.socket, certfile="./wasp.pem", server_side = True)
    logging.info('Starting http server listening on port ' + str(port) + "\n")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    logging.info('Stopping httpd...\n')

if __name__ == '__main__':
    from sys import argv

    if len(argv) == 2:
        run(useSsl=(argv[1] == "ssl"))
    else:
        run()