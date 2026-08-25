#!/usr/bin/env python3
"""
app.py — Container CENTRAL (lives.f5sites.com).

Não roda ffmpeg nem toca vídeo nenhum. Só recebe, via HTTP, o
status de cada container "streamer" (uma live/canal cada) e
mostra tudo agregado num dashboard em "/".

Cada streamer reporta periodicamente via:
    POST /report/{stream_name}
    body JSON: {kind, title, channel, episode, date, started_at}

Se um streamer parar de reportar (crash, deploy, etc.), o
dashboard marca ele como "offline" depois de OFFLINE_THRESHOLD
segundos sem heartbeat -- não precisa de configuração prévia de
quais streamers existem, eles se "auto-registram" no primeiro
report.
"""

import os
import threading
from datetime import datetime, timezone

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI()

OFFLINE_THRESHOLD_SECONDS = float(os.getenv("OFFLINE_THRESHOLD_SECONDS", "20"))

# stream_name -> {"kind", "title", "channel", "episode", "date",
#                 "started_at" (str iso ou None), "last_heartbeat" (datetime)}
_streams_lock = threading.Lock()
_streams = {}


def _escape_html(text):
    if text is None:
        return ""
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _format_elapsed(iso_str, reference=None):
    if not iso_str:
        return "—"

    try:
        started = datetime.fromisoformat(iso_str)
    except ValueError:
        return "—"

    reference = reference or datetime.now(timezone.utc)
    seconds = int((reference - started).total_seconds())
    minutes, seconds = divmod(max(seconds, 0), 60)
    hours, minutes = divmod(minutes, 60)

    if hours:
        return f"{hours}h {minutes}min {seconds}s"
    if minutes:
        return f"{minutes}min {seconds}s"
    return f"{seconds}s"


@app.post("/report/{stream_name}")
async def report(stream_name: str, request: Request):
    payload = await request.json()

    with _streams_lock:
        _streams[stream_name] = {
            "kind": payload.get("kind"),
            "title": payload.get("title"),
            "channel": payload.get("channel"),
            "episode": payload.get("episode"),
            "date": payload.get("date"),
            "started_at": payload.get("started_at"),
            "last_heartbeat": datetime.now(timezone.utc),
        }

    return JSONResponse({"ok": True})


@app.get("/api/status")
def api_status():
    """Mesmos dados do dashboard, em JSON -- útil pra integrações."""
    now = datetime.now(timezone.utc)

    with _streams_lock:
        snapshot = dict(_streams)

    result = {}
    for name, data in snapshot.items():
        is_online = (now - data["last_heartbeat"]).total_seconds() <= OFFLINE_THRESHOLD_SECONDS
        result[name] = {
            **{k: v for k, v in data.items() if k != "last_heartbeat"},
            "online": is_online,
            "last_heartbeat": data["last_heartbeat"].isoformat(),
        }

    return result


@app.get("/", response_class=HTMLResponse)
def index():
    now = datetime.now(timezone.utc)

    with _streams_lock:
        snapshot = dict(_streams)

    cards = []
    for name in sorted(snapshot.keys()):
        data = snapshot[name]
        seconds_since_heartbeat = (now - data["last_heartbeat"]).total_seconds()
        is_online = seconds_since_heartbeat <= OFFLINE_THRESHOLD_SECONDS

        status_class = "online" if is_online else "offline"
        status_label = "AO VIVO" if is_online else "OFFLINE"

        if data["title"]:
            kind_label = "Bumper (intro/outro)" if data["kind"] == "bumper" else "Vídeo"
            body_html = f"""
            <h2>{_escape_html(data['title'])}</h2>
            <p class="meta">
              {_escape_html(data['channel'])}
              {' • ' + _escape_html(data['episode']) if data['episode'] else ''}
              {' • ' + _escape_html(data['date']) if data['date'] else ''}
            </p>
            <p class="sub">{kind_label} · há {_format_elapsed(data['started_at'], now)}</p>
            """
        else:
            body_html = '<h2 class="dim">Sem informação de vídeo ainda</h2>'

        cards.append(f"""
        <div class="card {status_class}">
          <div class="card-header">
            <span class="stream-name">{_escape_html(name)}</span>
            <span class="badge badge-{status_class}">{status_label}</span>
          </div>
          {body_html}
          <p class="heartbeat">Último report há {int(seconds_since_heartbeat)}s</p>
        </div>
        """)

    cards_html = "\n".join(cards) if cards else (
        '<p class="dim">Nenhuma live reportou status ainda. '
        'Confira se os streamers têm CENTRAL_API_URL configurado.</p>'
    )

    return f"""<!DOCTYPE html>
<html lang="pt-br">
<head>
  <meta charset="utf-8">
  <meta http-equiv="refresh" content="10">
  <title>Lives — Painel Central</title>
  <style>
    body {{
      background: #0f0f12; color: #eee;
      font-family: -apple-system, Segoe UI, Roboto, Arial, sans-serif;
      margin: 0; padding: 40px 24px;
    }}
    .container {{ max-width: 1100px; margin: 0 auto; }}
    h1 {{ font-size: 18px; color: #999; font-weight: 600;
          text-transform: uppercase; letter-spacing: 0.05em;
          margin-bottom: 24px; }}
    .grid {{
      display: grid; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
      gap: 20px;
    }}
    .card {{
      background: #1a1a1f; border: 1px solid #2a2a30; border-radius: 12px;
      padding: 20px 24px;
    }}
    .card.offline {{ opacity: 0.55; }}
    .card-header {{
      display: flex; justify-content: space-between; align-items: center;
      margin-bottom: 12px;
    }}
    .stream-name {{ font-weight: 700; font-size: 15px; color: #ccc; }}
    .badge {{
      font-weight: 700; font-size: 11px; letter-spacing: 0.05em;
      padding: 4px 10px; border-radius: 4px;
    }}
    .badge-online {{ background: #c81e1e; color: white; }}
    .badge-offline {{ background: #444; color: #ccc; }}
    .card h2 {{ margin: 0 0 6px 0; font-size: 19px; }}
    .card h2.dim {{ color: #777; font-weight: 400; font-size: 15px; }}
    .card .meta {{ color: #bbb; margin: 0 0 4px 0; font-size: 14px; }}
    .card .sub {{ color: #888; font-size: 13px; margin: 0; }}
    .card .heartbeat {{ color: #555; font-size: 11px; margin: 12px 0 0 0; }}
    .dim {{ color: #777; }}
  </style>
</head>
<body>
  <div class="container">
    <h1>Lives ({len(snapshot)})</h1>
    <div class="grid">
      {cards_html}
    </div>
  </div>
</body>
</html>"""
