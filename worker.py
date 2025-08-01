import json
import time

import mureq
import redis

PRIMARY_URL = 'http://localhost:8001/payments'
FALLBACK_URL = 'http://localhost:8002/payments'
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_QUEUE = 'payments'

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def send_data_to_api(url, data):
    try:
        resp = mureq.post(url, json=data)
        if 200 <= resp.status_code < 300:
            print(f'✅ Sucesso ao enviar para {url}')
            return True
        else:
            print(f'❌ Erro HTTP {resp.status_code} ao enviar para {url}')
            return False
    except Exception as e:
        print(f'❌ Falha de conexão com {url}: {e}')
        return False

def send_with_circuit_breaker(data):
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
    data["requestedAt"] = now_iso

    print("🔁 Tentando enviar para a API primária...")
    success = send_data_to_api(PRIMARY_URL, data)

    if success:
        try:
            r.set(f"payment:{data['correlationId']}", json.dumps(data))
        except Exception as e:
            print(f"❌ Erro salvando cache Redis: {e}")
        return

    print("⚠️ Enviando para a API fallback...")
    fallback_success = send_data_to_api(FALLBACK_URL, data)

    if fallback_success:
        try:
            r.set(f"payment:{data['correlationId']}_fallback", json.dumps(data))
        except Exception as e:
            print(f"❌ Erro salvando cache fallback Redis: {e}")

def worker_loop():
    print("👷 Worker iniciado. Aguardando pagamentos...")
    while True:
        item = r.brpop(REDIS_QUEUE, timeout=3)
        if item:
            _, json_str = item  # r.brpop retorna tupla (queue, value)
            try:
                data = json.loads(json_str)
                send_with_circuit_breaker(data)
            except Exception as e:
                print(f"❌ Erro ao processar item da fila: {e}")

if __name__ == "__main__":
    worker_loop()
