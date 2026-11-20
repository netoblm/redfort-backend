"""
REDFORT API — backend de stress test real
FastAPI + hey (HTTP load generator)
Deploy: Render.com Web Service
"""

import asyncio
import json
import os
import re
import shutil
import subprocess
import tempfile
from typing import AsyncGenerator

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, HttpUrl

app = FastAPI(title="REDFORT API", version="1.0.0")

# CORS aberto — painel roda em domínio diferente
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class FloodRequest(BaseModel):
    target: str
    users: int = 50        # concorrência (-c)
    requests: int = 5000   # total de requisições (-n)
    duration: int = 30     # duração em segundos (-z)
    method: str = "GET"


def _hey_path() -> str:
    """Localiza hey no PATH ou em /tmp/hey."""
    p = shutil.which("hey")
    if p:
        return p
    local = "/tmp/hey"
    if os.path.isfile(local) and os.access(local, os.X_OK):
        return local
    return ""


def _install_hey() -> bool:
    """Baixa hey para /tmp se não estiver disponível."""
    import urllib.request, stat
    url = "https://hey-release.s3.us-east-2.amazonaws.com/hey_linux_amd64"
    dest = "/tmp/hey"
    try:
        urllib.request.urlretrieve(url, dest)
        os.chmod(dest, os.stat(dest).st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
        return True
    except Exception:
        return False


def _parse_hey_output(raw: str) -> dict:
    """Extrai métricas do output do hey."""
    metrics = {}
    patterns = {
        "total_requests": r"(\d+) requests in",
        "rps":            r"Requests/sec:\s+([\d.]+)",
        "avg_ms":         r"Average:\s+([\d.]+) secs",
        "p50_ms":         r"50% in ([\d.]+) secs",
        "p90_ms":         r"90% in ([\d.]+) secs",
        "p99_ms":         r"99% in ([\d.]+) secs",
        "fastest_ms":     r"Fastest:\s+([\d.]+) secs",
        "slowest_ms":     r"Slowest:\s+([\d.]+) secs",
        "total_data":     r"Total data:\s+(.+)",
        "size_req":       r"Size/request:\s+(.+)",
    }
    for key, pat in patterns.items():
        m = re.search(pat, raw)
        if m:
            val = m.group(1)
            # converter secs → ms para latências
            if key.endswith("_ms") and key != "total_data":
                try:
                    val = round(float(val) * 1000, 1)
                except ValueError:
                    pass
            metrics[key] = val

    # status codes
    status_block = re.search(r"\[(\d+)\]\s+(\d+) responses", raw)
    if status_block:
        metrics["status_2xx"] = status_block.group(2)

    # erros
    err_m = re.search(r"Error distribution:(.*?)$", raw, re.DOTALL)
    if err_m:
        metrics["errors"] = err_m.group(1).strip()[:200]

    return metrics


async def _stream_flood(req: FloodRequest) -> AsyncGenerator[str, None]:
    """Executa hey e faz streaming do resultado via SSE."""

    def sse(data: dict) -> str:
        return f"data: {json.dumps(data)}\n\n"

    # garantir hey disponível
    hey = _hey_path()
    if not hey:
        yield sse({"type": "log", "msg": "[*] hey não encontrado — instalando...", "level": "info"})
        ok = await asyncio.get_event_loop().run_in_executor(None, _install_hey)
        if not ok:
            yield sse({"type": "error", "msg": "[!] falha ao instalar hey — verifique conectividade do Render"})
            return
        hey = "/tmp/hey"
        yield sse({"type": "log", "msg": "[+] hey instalado com sucesso", "level": "success"})

    # validar target
    target = req.target
    if not target.startswith("http"):
        target = "https://" + target

    users = max(1, min(req.users, 500))
    duration = max(5, min(req.duration, 120))

    cmd = [
        hey,
        "-c", str(users),          # concorrência
        "-z", f"{duration}s",       # duração
        "-m", req.method,
        "-disable-keepalive",       # força novas conexões TCP
        "-H", "User-Agent: RedfortProbe/1.0",
        target
    ]

    yield sse({"type": "log", "msg": f"[*] iniciando flood real: {users} conn · {duration}s · {target}", "level": "info"})
    yield sse({"type": "log", "msg": f"[*] cmd: {' '.join(cmd)}", "level": "muted"})

    # progresso fake enquanto hey roda (hey não tem output em tempo real)
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    # tick de progresso a cada 3s enquanto espera
    tick = 0
    while proc.returncode is None:
        await asyncio.sleep(3)
        tick += 3
        if tick <= duration:
            yield sse({"type": "progress", "elapsed": tick, "total": duration,
                       "msg": f"[~] rodando... {tick}/{duration}s", "level": "muted"})

    stdout, stderr = await proc.communicate()
    raw = stdout.decode("utf-8", errors="replace")
    err = stderr.decode("utf-8", errors="replace")

    if proc.returncode != 0 and not raw:
        yield sse({"type": "error", "msg": f"[!] hey falhou: {err[:300]}"})
        return

    # parsear e emitir métricas
    metrics = _parse_hey_output(raw)
    yield sse({"type": "log", "msg": f"[*] concluído — {metrics.get('total_requests','?')} req · {metrics.get('rps','?')} req/s", "level": "success"})
    yield sse({"type": "log", "msg": f"[*] avg={metrics.get('avg_ms','?')}ms p50={metrics.get('p50_ms','?')}ms p99={metrics.get('p99_ms','?')}ms", "level": "info"})
    yield sse({"type": "log", "msg": f"[*] fastest={metrics.get('fastest_ms','?')}ms slowest={metrics.get('slowest_ms','?')}ms", "level": "info"})

    if metrics.get("errors"):
        yield sse({"type": "log", "msg": f"[!] erros: {metrics['errors']}", "level": "warning"})

    # calcular crash prob real baseada em p99
    p99 = metrics.get("p99_ms", 0)
    try:
        p99 = float(p99)
        if p99 < 500:       crash_prob = 5
        elif p99 < 1000:    crash_prob = 25
        elif p99 < 2000:    crash_prob = 55
        elif p99 < 5000:    crash_prob = 80
        else:               crash_prob = 95
    except (TypeError, ValueError):
        crash_prob = 0

    yield sse({"type": "result", "metrics": metrics, "crash_prob": crash_prob, "raw": raw[:2000]})
    yield sse({"type": "done"})


@app.get("/health")
def health():
    return {"status": "ok", "hey": bool(_hey_path())}


@app.post("/flood")
async def flood(req: FloodRequest):
    """Executa flood real via hey e retorna resultado em SSE streaming."""
    return StreamingResponse(
        _stream_flood(req),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        }
    )


@app.get("/")
def root():
    return {"service": "REDFORT API", "version": "1.0.0", "endpoints": ["/flood", "/health"]}
