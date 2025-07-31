import json
import socket
from http.server import BaseHTTPRequestHandler, HTTPServer

# Redis
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_QUEUE = 'payments'

class SimpleHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/payment":
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not Found")
            return

        content_length = int(self.headers.get("Content-Length", 0))
        raw_body = self.rfile.read(content_length)
        try:
            data = json.loads(raw_body)
            self.push_to_redis_queue(json.dumps(data))
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")
        except Exception as e:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(f"Error: {str(e)}".encode())
        
    def push_to_redis_queue(self, item):
        with socket.create_connection((REDIS_HOST, REDIS_PORT)) as sock:
            # Comando Redis LPUSH payments "item"
            command = f"*3\r\n$5\r\nLPUSH\r\n${len(REDIS_QUEUE)}\r\n{REDIS_QUEUE}\r\n${len(item)}\r\n{item}\r\n"
            sock.sendall(command.encode())


def run_server():
    server_address = ('', 8080)
    httpd = HTTPServer(server_address, SimpleHandler)
    
    print('------> 🚀 Servidor rodando!')
    
    httpd.serve_forever()



if __name__ == '__main__':
    run_server()
    
    
