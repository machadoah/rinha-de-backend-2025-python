import http.client
import json
import socket
from urllib.parse import urlparse

# Payment processors
PRIMARY_URL = 'http://localhost:8001'
FALLBACK_URL = 'http://localhost:8002'
REDIS_HOST = "localhost"
REDIS_PORT = 6379

# circuit-braker
def send_data_to_api(url, data):
    parsed = urlparse(url)
    conn = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=2)
    
    try:
        payload = json.dumps(data)
        headers = {"Content-Type": "application/json"}
        conn.request('POST', parsed.path, body=payload, headers=headers)
        response = conn.getresponse()
        
        if 200 <= response.status < 300:
            print('✅ Sucesso! Enviado para o payment processor!')
            return True
        else:
            print(f'❌ Erro HTTP {response.status}!')
            return False
    except Exception as e:
        print(f'❌ Falha de conexão! {e}')
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


# send to PP
def enviar_com_circuit_braker(data):
    print("🔁 Tentando enviar para o PP (DEFAULT)...")
    is_sucess_default = send_data_to_api(PRIMARY_URL, data)
    
    if is_sucess_default:
        redis_set(f'payment:{data['currelationId']}', json.dumps(data))
        return
    
    print("⚠️ Enviando para o PP (FALLBACK)...")
    is_sucess_fallback = send_data_to_api(FALLBACK_URL, data)

    if is_sucess_fallback:
        redis_set(FALLBACK_URL, data)


