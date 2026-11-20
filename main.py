"""
REDFORT API — backend de stress test real
FastAPI + curl paralelo (sem dependência externa)
"""
import asyncio, json, os, shutil, subprocess, time
from typing import AsyncGenerator
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

app = FastAPI(title="REDFORT API", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class FloodRequest(BaseModel):
    target: str
    users: int = 50
    duration: int = 30
    method: str = "GET"

def sse(data: dict) -> str:
    return f"data: {json.dumps(data)}\n\n"

def _normalize_target(target: str) -> str:
    t = target.strip()
    if not t.startswith("http://") and not t.startswith("https://"):
        t = "https://" + t
    return t

async def _stream_flood(req: FloodRequest) -> AsyncGenerator[str, None]:
    target = _normalize_target(req.target)
    users = max(1, min(req.users, 200))
    duration = max(5, min(req.duration, 120))
    method = req.method.upper() if req.method.upper() in ("GET", "POST", "HEAD") else "GET"

    yield sse({"type": "log", "msg": f"[*] alvo: {target}"})
    yield sse({"type": "log", "msg": f"[*] conexões: {users} · duração: {duration}s · método: {method}"})

    # verifica curl
    curl_path = shutil.which("curl")
    if not curl_path:
        yield sse({"type": "error", "msg": "[!] curl não encontrado no servidor"})
        return

    yield sse({"type": "log", "msg": f"[+] usando curl ({curl_path}) — flood server-side real"})
    yield sse({"type": "log", "msg": "[*] iniciando flood..."})

    start = time.time()
    total_requests = 0
    total_errors = 0
    latencies = []

    # semáforo pra controlar concorrência
    sem = asyncio.Semaphore(users)

    async def one_request():
        nonlocal total_requests, total_errors
        async with sem:
            t0 = time.time()
            try:
                proc = await asyncio.create_subprocess_exec(
                    curl_path,
                    "-s", "-o", "/dev/null", "-w", "%{http_code}:%{time_total}",
                    "-X", method,
                    "--max-time", "10",
                    "--connect-timeout", "5",
                    "--no-keepalive",
                    target,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL
                )
                stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=12)
                out = stdout.decode().strip()
                if ":" in out:
                    code, t_str = out.split(":", 1)
                    lat = float(t_str) * 1000
                    latencies.append(lat)
                    if int(code) >= 500:
                        total_errors += 1
                total_requests += 1
            except Exception:
                total_errors += 1
                total_requests += 1

    end_time = start + duration
    batch_size = users
    last_progress = 0

    while time.time() < end_time:
        # dispara batch de requisições paralelas
        tasks = [asyncio.create_task(one_request()) for _ in range(batch_size)]
        await asyncio.gather(*tasks, return_exceptions=True)

        elapsed = time.time() - start
        pct = min(100, int(elapsed / duration * 100))

        if pct != last_progress:
            last_progress = pct
            rps = total_requests / max(elapsed, 0.1)
            yield sse({
                "type": "progress",
                "pct": pct,
                "elapsed": int(elapsed),
                "total": duration,
                "requests": total_requests,
                "rps": round(rps, 1)
            })

        if time.time() >= end_time:
            break

    # métricas finais
    elapsed = time.time() - start
    rps = total_requests / max(elapsed, 0.1)
    latencies.sort()

    def pct_lat(p):
        if not latencies:
            return 0
        idx = int(len(latencies) * p / 100)
        return round(latencies[min(idx, len(latencies)-1)], 1)

    avg = round(sum(latencies) / len(latencies), 1) if latencies else 0
    p50 = pct_lat(50)
    p90 = pct_lat(90)
    p99 = pct_lat(99)

    # crash probability baseada em p99
    crash_prob = 0
    if p99 > 0:
        if p99 < 500:    crash_prob = 5
        elif p99 < 1000: crash_prob = 25
        elif p99 < 2000: crash_prob = 55
        elif p99 < 5000: crash_prob = 80
        else:            crash_prob = 95

    # boost se muitos erros 5xx
    if total_requests > 0 and total_errors / total_requests > 0.1:
        crash_prob = min(99, crash_prob + 20)

    yield sse({"type": "log", "msg": f"[+] flood completo · {total_requests} req · {round(rps,1)} rps · avg {avg}ms"})
    yield sse({
        "type": "result",
        "metrics": {
            "total_requests": total_requests,
            "rps": round(rps, 1),
            "avg_ms": avg,
            "p50_ms": p50,
            "p90_ms": p90,
            "p99_ms": p99,
            "errors": total_errors,
            "duration_s": round(elapsed, 1)
        },
        "crash_prob": crash_prob,
        "raw": f"total={total_requests} rps={round(rps,1)} avg={avg}ms p50={p50}ms p90={p90}ms p99={p99}ms errors={total_errors}"
    })
    yield sse({"type": "done"})


@app.get("/health")
def health():
    curl_ok = bool(shutil.which("curl"))
    return {"status": "ok", "hey": False, "curl": curl_ok, "engine": "curl-parallel"}

@app.post("/flood")
async def flood(req: FloodRequest):
    return StreamingResponse(
        _stream_flood(req),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )

@app.get("/")
def root():
    return {"service": "REDFORT API", "version": "2.0.0", "engine": "curl-parallel", "endpoints": ["/flood", "/health"]}
