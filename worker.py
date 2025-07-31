import http.client
import json
import socket
import time
from urllib.parse import urlparse

PRIMARY_URL = 'http://localhost:8001/payments'
FALLBACK_URL = 'http://localhost:8002/payments'
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_QUEUE = "payments"

# circuit-breaker
def send_data_to_api(url, data):
    parsed = urlparse(url)
    conn = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=2)

    try:
        payload = json.dumps(data)
        headers = {"Content-Type": "application/json"}
        conn.request('POST', parsed.path, body=payload, headers=headers)
        response = conn.getresponse()

        if 200 <= response.status < 300:
            print(f'✅ Sucesso ao enviar para {url}')
            return True
        else:
            print(f'❌ Erro HTTP {response.status} ao enviar para {url}')
            return False
    except Exception as e:
        print(f'❌ Falha de conexão com {url}: {e}')
        return False
    finally:
        conn.close()

# redis persistence
def redis_set(key, value):
    encoded = value.encode() if isinstance(value, str) else value
    command = (
        f"*3\r\n$3\r\nSET\r\n${len(key)}\r\n{key}\r\n${len(encoded)}\r\n{encoded.decode()}\r\n"
    )

    try:
        with socket.create_connection((REDIS_HOST, REDIS_PORT)) as sock:
            sock.sendall(command.encode())
            response = sock.recv(1024)
            if b"+OK" in response:
                print(f"📦 Cache salvo com sucesso: {key}")
                return True
            else:
                print(f"❌ Erro ao salvar no Redis: {response}")
                return False
    except Exception as e:
        print(f"❌ Erro de conexão com Redis: {e}")
        return False

# get Redis
def redis_brpop(queue_name, timeout=5):
    cmd = f"*3\r\n$6\r\nBRPOP\r\n${len(queue_name)}\r\n{queue_name}\r\n${len(str(timeout))}\r\n{timeout}\r\n"
    try:
        with socket.create_connection((REDIS_HOST, REDIS_PORT)) as sock:
            sock.sendall(cmd.encode())
            resp = sock.recv(4096)
            if resp.startswith(b"$-1"):
                return None
            parts = resp.split(b"\r\n")
            if len(parts) >= 5:
                return parts[3].decode()
            return None
    except Exception as e:
        print("❌ Erro redis BRPOP:", e)
        return None


# send to PP
def enviar_com_circuit_breaker(data):
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
    data["requestedAt"] = now_iso

    print("🔁 Tentando enviar para a API primária...")
    success = send_data_to_api(PRIMARY_URL, data)

    if success:
        redis_set(f"payment:{data['correlationId']}", json.dumps(data))
        return

    print("⚠️ Enviando para a API fallback...")
    fallback_success = send_data_to_api(FALLBACK_URL, data)

    if fallback_success:
        redis_set(f"payment:{data['correlationId']}_fallback", json.dumps(data))

def worker_loop():
    print("👷 Worker iniciado. Aguardando pagamentos...")
    while True:
        item = redis_brpop(REDIS_QUEUE, timeout=3)
        if item:
            try:
                data = json.loads(item)
                enviar_com_circuit_breaker(data)
            except Exception as e:
                print(f"❌ Erro ao processar item da fila: {e}")
        # sem sleep — o BRPOP já bloqueia por timeout e recomeça


if __name__ == "__main__":
    worker_loop()
