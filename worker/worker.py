import json
import os
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from http import HTTPStatus

import mureq
import redis

PAYMENT_PROCESSOR_DEFAULT = os.getenv("PAYMENT_PROCESSOR_DEFAULT", "http://localhost:8001/payments")
PAYMENT_PROCESSOR_FALLBACK = os.getenv("PAYMENT_PROCESSOR_FALLBACK", "http://localhost:8002/payments")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = 6379
REDIS_QUEUE = "payments"

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def send_data_to_api(url, data):
    try:
        print(f"📦 Enviando dados para {url}:\n{json.dumps(data, indent=2)}")

        resp = mureq.post(url, json=data)
        if HTTPStatus(resp.status_code).is_success:
            print(f"✅ Sucesso ao enviar para {url}")
            return True
        else:
            print(f"❌ Erro HTTP {resp.status_code} ao enviar para {url}")
            print(f"📨 Resposta: {resp.body.decode()}")
            return False
    except Exception as e:
        print(f"❌ Falha de conexão com {url}: {e}")
        return False


def send_with_circuit_breaker(data):
    now_iso = (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )
    data["requestedAt"] = now_iso
    
    
    if "amount" in data:
        data["amount"] = float(Decimal(str(data["amount"])).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


    print("🔁 Tentando enviar para a API primária...")
    success = send_data_to_api(PAYMENT_PROCESSOR_DEFAULT, data)

    if success:
        try:
            r.set(f"payment:{data['correlationId']}", json.dumps(data))
        except Exception as e:
            print(f"❌ Erro salvando cache Redis: {e}")
        return

    print("⚠️ Enviando para a API fallback...")
    fallback_success = send_data_to_api(PAYMENT_PROCESSOR_FALLBACK, data)

    if fallback_success:
        try:
            r.set(
                f"payment:{data['correlationId']}_fallback", json.dumps(data)
            )
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
