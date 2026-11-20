from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import httpx, asyncio, time

app = FastAPI(title="REDFORT Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"status": "online", "service": "REDFORT Backend"}

@app.get("/ping")
def ping():
    return {"status": "online", "ts": int(time.time())}

@app.get("/stress")
async def stress(url: str, users: int = 10, duration: int = 15):
    users    = max(1, min(users, 50))
    duration = max(5, min(duration, 60))

    total = 0
    ok    = 0
    fail  = 0
    times = []

    end_time = time.time() + duration

    async def fire(client: httpx.AsyncClient):
        nonlocal total, ok, fail
        t0 = time.time()
        try:
            r = await client.get(url, timeout=10, follow_redirects=True)
            elapsed = round((time.time() - t0) * 1000)
            times.append(elapsed)
            total += 1
            if r.status_code < 500:
                ok += 1
            else:
                fail += 1
        except Exception:
            total += 1
            fail  += 1

    async with httpx.AsyncClient() as client:
        while time.time() < end_time:
            batch = [fire(client) for _ in range(users)]
            await asyncio.gather(*batch)
            await asyncio.sleep(0.1)

    avg_ms    = round(sum(times) / len(times)) if times else 0
    fail_rate = round((fail / total) * 100, 1) if total else 0
    rps       = round(total / duration)

    if avg_ms < 300 and fail_rate < 5:
        verdict = "healthy"
    elif avg_ms < 800 and fail_rate < 20:
        verdict = "stressed"
    else:
        verdict = "overloaded"

    return {
        "total":     total,
        "ok":        ok,
        "fail":      fail,
        "avg_ms":    avg_ms,
        "fail_rate": fail_rate,
        "rps":       rps,
        "verdict":   verdict,
    }