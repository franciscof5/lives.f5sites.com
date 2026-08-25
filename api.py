#!/usr/bin/env python3
"""
api.py — Servidor web (FastAPI) com a rota "/": mostra o que
está tocando agora e a lista de vídeos, consultando o estado
compartilhado exposto por stream.py.

Iniciado como thread por live.py -- não é chamado diretamente.
"""

import os
import traceback
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import uvicorn

import stream

# ============================================================
# WEB — FastAPI com a rota "/" (lista de vídeos + tocando agora)
# ============================================================
#
# Servido em lives.f5sites.com (via proxy reverso apontando pra
# essa porta). Roda em thread separada, sem interferir no feeder
# nem no publisher.

API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "80"))

app = FastAPI()


def _format_elapsed(started_at):
    if started_at is None:
        return "—"

    seconds = int((datetime.now(timezone.utc) - started_at).total_seconds())
    minutes, seconds = divmod(max(seconds, 0), 60)
    hours, minutes = divmod(minutes, 60)

    if hours:
        return f"{hours}h {minutes}min {seconds}s"
    if minutes:
        return f"{minutes}min {seconds}s"
    return f"{seconds}s"


def _escape_html(text):
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def render_index_html():
    now_playing = stream.get_now_playing()
    all_videos = sorted(
        (
            p for p in stream.VIDEO_DIR.iterdir()
            if stream.VIDEO_DIR.exists()
            and p.is_file()
            and p.name.lower().endswith(stream.VIDEO_SUFFIX)
            and not p.name.startswith("_encode_")
        ) if stream.VIDEO_DIR.exists() else [],
        key=lambda p: p.name,
    )

    current_path = now_playing["video_path"]
    current_kind = now_playing["kind"]
    elapsed = _format_elapsed(now_playing["started_at"])

    # ---- bloco "tocando agora" ----
    if current_path is not None:
        meta = stream.parse_video_metadata(current_path)
        kind_label = "Bumper (intro/outro)" if current_kind == "bumper" else "Vídeo"
        now_playing_html = f"""
        <div class="now-playing">
          <span class="badge">AO VIVO</span>
          <h2>{_escape_html(meta['title'])}</h2>
          <p class="meta">
            {_escape_html(meta['channel'])}
            {' • ' + _escape_html(meta['episode']) if meta['episode'] else ''}
            {' • ' + _escape_html(meta['date']) if meta['date'] else ''}
          </p>
          <p class="sub">{kind_label} · há {elapsed}</p>
        </div>
        """
    else:
        now_playing_html = """
        <div class="now-playing">
          <span class="badge badge-off">AGUARDANDO</span>
          <h2>Nenhum vídeo tocando ainda</h2>
        </div>
        """

    # ---- lista completa ----
    rows = []
    for video in all_videos:
        meta = stream.parse_video_metadata(video)
        is_current = current_path is not None and video == current_path
        has_bumper = stream.bumper_cache_key(video).exists() if stream.BUMPER_ENABLED else None

        bumper_icon = ""
        if stream.BUMPER_ENABLED:
            bumper_icon = "✅" if has_bumper else "⏳"

        row_class = "current" if is_current else ""
        marker = "▶" if is_current else ""

        rows.append(f"""
        <tr class="{row_class}">
          <td>{marker}</td>
          <td>{_escape_html(meta['title'])}</td>
          <td>{_escape_html(meta['channel'])}</td>
          <td>{_escape_html(meta['episode'])}</td>
          <td>{_escape_html(meta['date'])}</td>
          <td class="center">{bumper_icon}</td>
        </tr>
        """)

    rows_html = "\n".join(rows) if rows else (
        '<tr><td colspan="6">Nenhum vídeo encontrado em ' + _escape_html(str(stream.VIDEO_DIR)) + '</td></tr>'
    )

    return f"""<!DOCTYPE html>
<html lang="pt-br">
<head>
  <meta charset="utf-8">
  <meta http-equiv="refresh" content="10">
  <title>Elenco B Cast — Live 24/7</title>
  <style>
    body {{
      background: #0f0f12; color: #eee;
      font-family: -apple-system, Segoe UI, Roboto, Arial, sans-serif;
      margin: 0; padding: 40px 24px;
    }}
    .container {{ max-width: 960px; margin: 0 auto; }}
    .now-playing {{
      background: #1a1a1f; border: 1px solid #2a2a30; border-radius: 12px;
      padding: 24px 28px; margin-bottom: 32px;
    }}
    .badge {{
      display: inline-block; background: #c81e1e; color: white;
      font-weight: 700; font-size: 12px; letter-spacing: 0.05em;
      padding: 4px 10px; border-radius: 4px; margin-bottom: 12px;
    }}
    .badge-off {{ background: #444; }}
    .now-playing h2 {{ margin: 0 0 6px 0; font-size: 26px; }}
    .now-playing .meta {{ color: #bbb; margin: 0 0 4px 0; }}
    .now-playing .sub {{ color: #888; font-size: 14px; margin: 0; }}
    h1 {{ font-size: 18px; color: #999; font-weight: 600;
          text-transform: uppercase; letter-spacing: 0.05em; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th {{ text-align: left; color: #888; font-weight: 600; padding: 8px 10px;
          border-bottom: 1px solid #2a2a30; }}
    td {{ padding: 10px; border-bottom: 1px solid #1e1e24; }}
    td.center {{ text-align: center; }}
    tr.current {{ background: #1a2a1a; }}
    tr.current td {{ color: #baffba; font-weight: 600; }}
  </style>
</head>
<body>
  <div class="container">
    {now_playing_html}
    <h1>Playlist ({len(all_videos)} vídeos)</h1>
    <table>
      <thead>
        <tr>
          <th></th><th>Título</th><th>Canal</th><th>Episódio</th><th>Data</th><th>Bumper</th>
        </tr>
      </thead>
      <tbody>
        {rows_html}
      </tbody>
    </table>
  </div>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
def index():
    return render_index_html()


def start_api_server():
    """
    Roda numa thread separada. Se uvicorn.run() lançar qualquer
    exceção (porta ocupada, falta de permissão, etc.), o traceback
    completo é impresso com um marcador [API][FATAL] fácil de
    localizar nos logs -- caso contrário, uma exceção numa thread
    só aparece no stderr sem contexto e pode passar despercebida.
    """

    try:
        print(f"[API] Servindo em http://{API_HOST}:{API_PORT}/")
        uvicorn.run(app, host=API_HOST, port=API_PORT, log_level="warning")
    except Exception:
        print("[API][FATAL] Servidor da API caiu ao iniciar:")
        traceback.print_exc()


