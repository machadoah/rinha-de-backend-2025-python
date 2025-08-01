import json

import redis
from bottle import Bottle, request, response, run

# Redis Config
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_QUEUE = 'payments'

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

app = Bottle()

@app.post('/payments')
def handle_payment():
    data = request.json
    if not data:
        response.status = 400
        return {"error": "JSON inválido ou ausente"}

    try:
        # Push na fila Redis
        r.lpush(REDIS_QUEUE, json.dumps(data))
        return {"status": "OK"}
    except Exception as e:
        response.status = 500
        return {"error": str(e)}

@app.get('/me')
def about_my_self():
    return {
        'name': 'Antonio Henrique Machado',
        'username': 'machadoah',
        'github': 'https://github.com/machadoah/',
        'linkedin': 'https://www.linkedin.com/in/machadoah/'
    }

if __name__ == "__main__":
    print("/n🍾 Servidor Bottle")
    run(app, host='0.0.0.0', port=8080)
