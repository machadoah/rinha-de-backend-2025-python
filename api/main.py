import json
import os
from datetime import datetime
from decimal import Decimal

import redis
from bottle import Bottle, request, response, run

# Redis Config
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = 6379
REDIS_QUEUE = "payments"

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

app = Bottle()


@app.post("/payments")
def handle_payment():
    data = request.json
    if not data:
        response.status = 400
        return {"error": "JSON inválido ou ausente"}

    try:
        # Push na fila Redis
        r.lpush(REDIS_QUEUE, json.dumps(data))
        response.status = 202
        return {"message": "🤑 Pagamento recebido para processamento"}
    except Exception as e:
        response.status = 500
        return {"error": str(e)}


@app.get("/payments-summary")
def payments_summary():
    from_iso = request.query.get("from")
    to_iso = request.query.get("to")

    def parse_iso(s):
        return datetime.fromisoformat(s.replace("Z", "+00:00"))

    dt_from = parse_iso(from_iso) if from_iso else None
    dt_to = parse_iso(to_iso) if to_iso else None

    summary = {
        "default": {"totalRequests": 0, "totalAmount": Decimal("0")},
        "fallback": {"totalRequests": 0, "totalAmount": Decimal("0")},
    }

    def process_category(category, pattern):
        keys = list(r.scan_iter(pattern))
        if not keys:
            return

        pipe = r.pipeline()
        for key in keys:
            pipe.get(key)
        values = pipe.execute()

        for key, raw in zip(keys, values):
            if not raw:
                continue
            try:
                payment = json.loads(raw)

                if category == "default" and key.endswith("_fallback"):
                    continue
                if category == "fallback" and not key.endswith("_fallback"):
                    continue

                dt_req = None
                if "requestedAt" in payment:
                    dt_req = parse_iso(payment["requestedAt"])

                if dt_req and dt_from and dt_req < dt_from:
                    continue
                if dt_req and dt_to and dt_req > dt_to:
                    continue

                amt = payment.get("amount", 0)
                amt = Decimal(str(amt))

                summary[category]["totalRequests"] += 1
                summary[category]["totalAmount"] += amt
            except Exception as e:
                print(f"⚠️ Erro ao processar {key}: {e}")

    process_category("default", "payment:*")
    process_category("fallback", "payment:*_fallback")

    return {
        "default": {
            "totalRequests": summary["default"]["totalRequests"],
            "totalAmount": float(summary["default"]["totalAmount"]),
        },
        "fallback": {
            "totalRequests": summary["fallback"]["totalRequests"],
            "totalAmount": float(summary["fallback"]["totalAmount"]),
        },
    }


@app.get("/me")
def about_my_self():
    return {
        "name": "Antonio Henrique Machado",
        "username": "machadoah",
        "github": "https://github.com/machadoah/",
        "linkedin": "https://www.linkedin.com/in/machadoah/",
    }


if __name__ == "__main__":
    print("/n🍾 Servidor Bottle")
    run(app, host="0.0.0.0", port=8080)
