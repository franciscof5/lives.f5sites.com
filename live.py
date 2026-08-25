#!/usr/bin/env python3

import os
import re
import time
import signal
import hashlib
import subprocess
import threading
import concurrent.futures
from pathlib import Path
from datetime import datetime, timezone

import dotenv
from PIL import Image, ImageDraw, ImageFont
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import uvicorn

dotenv.load_dotenv()


# ============================================================
# CONFIG
# ============================================================

VIDEO_DIR = Path(os.getenv("VIDEO_DIR", "/videos"))

YOUTUBE_RTMP_URL = os.getenv(
    "YOUTUBE_RTMP_URL",
    "rtmp://a.rtmp.youtube.com/live2"
)

YOUTUBE_STREAM_KEY = os.getenv("YOUTUBE_STREAM_KEY")

RESTART_DELAY = int(os.getenv("RESTART_DELAY", "5"))

VIDEO_SUFFIX = "_small_cut_subtitles.mp4"

FIFO_PATH = Path(os.getenv("FIFO_PATH", "/tmp/youtube_stream.fifo"))

ORDER_VIDEO_FEED = "random" #os.getenv("ORDER_VIDEO_FEED", "Alphabetic").strip().lower()

VALID_ORDER_MODES = {
    "alphabetic",  # A-Z pelo nome do arquivo (comportamento atual/padrão)
    "random",      # embaralhado, reordenado a cada volta completa da playlist
    "newest",      # vídeos mais recentes primeiro (por data de modificação)
    "oldest",      # vídeos mais antigos primeiro (por data de modificação)
}

if ORDER_VIDEO_FEED not in VALID_ORDER_MODES:
    print(
        f"[WARN] ORDER_VIDEO_FEED='{ORDER_VIDEO_FEED}' inválido. "
        f"Usando 'alphabetic'. Opções válidas: {', '.join(sorted(VALID_ORDER_MODES))}"
    )
    ORDER_VIDEO_FEED = "alphabetic"

_file_state_cache = {}  # path (str) -> {"size", "gop_checked", "gop_ok"}
# ============================================================
# STATE
# ============================================================

running = True
publisher_process = None

# Estado do "que está tocando agora", exposto pela API/rota web.
# Atualizado pelo feeder_loop (thread do feeder), lido pela thread
# do servidor web -- por isso o lock.
_now_playing_lock = threading.Lock()
_now_playing = {
    "kind": None,       # "bumper" ou "video"
    "video_path": None, # Path do vídeo relacionado (mesmo durante o bumper)
    "started_at": None, # datetime UTC de quando começou a tocar esse trecho
}


def set_now_playing(kind, video_path):
    with _now_playing_lock:
        _now_playing["kind"] = kind
        _now_playing["video_path"] = video_path
        _now_playing["started_at"] = datetime.now(timezone.utc)


def get_now_playing():
    with _now_playing_lock:
        return dict(_now_playing)


# ============================================================
# SIGNALS
# ============================================================

def handle_signal(signum, frame):
    global running

    print(f"\n[SYSTEM] Signal {signum} recebido. Encerrando...")

    running = False

    if publisher_process and publisher_process.poll() is None:

        print("[SYSTEM] Encerrando Publisher...")

        publisher_process.terminate()

        try:
            publisher_process.wait(timeout=3)

        except subprocess.TimeoutExpired:

            print("[SYSTEM] Publisher não encerrou. Forçando...")

            publisher_process.kill()
            publisher_process.wait()


signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)


# ============================================================
# SLEEP
# ============================================================

def interruptible_sleep(seconds):
    end_time = time.time() + seconds
    while running and time.time() < end_time:
        time.sleep(0.1)


# ============================================================
# FIFO
# ============================================================

def ensure_fifo():
    """
    Cria a FIFO uma única vez, no início. Ela NÃO é recriada
    depois — se o publisher cair e reconectar, ele volta a ler
    da mesma FIFO, sem quebrar o feeder.
    """

    if not FIFO_PATH.exists():
        os.mkfifo(str(FIFO_PATH))
        print(f"[SYSTEM] FIFO criada: {FIFO_PATH}")


# ============================================================
# VIDEO DISCOVERY
# ============================================================

# ============================================================
# STATE (adicionar junto das outras)
# ============================================================

# ============================================================
# CONFIG (adicionar junto das outras, no topo do arquivo)
# ============================================================

MAX_KEYFRAME_GAP_SECONDS = float(os.getenv("MAX_KEYFRAME_GAP_SECONDS", "6"))

MIN_FILE_AGE_SECONDS = float(os.getenv("MIN_FILE_AGE_SECONDS", "10"))
CHECKS_FILE_READY = False #os.getenv("CHECKS_FILE_READY", "True").lower() not in ("false", "0", "")
# ============================================================
# VALIDAÇÃO DE GOP
# ============================================================

def is_valid_gop(video_path, max_gap=MAX_KEYFRAME_GAP_SECONDS):
    """
    Verifica se o vídeo tem keyframes frequentes o suficiente
    pra ser aceito no live (streamer usa -c copy, não corrige
    isso mais). Retorna True se o maior intervalo entre
    keyframes for <= max_gap segundos.
    """

    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v", "error",
                "-select_streams", "v:0",
                # pts_time é o nome atual (ffmpeg >= 5.0);
                # pkt_pts_time é o nome antigo (ffmpeg < 5.0).
                # Pedindo os dois cobre qualquer versão.
                "-show_entries", "frame=key_frame,pts_time,pkt_pts_time",
                "-of", "csv=print_section=0",
                str(video_path),
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )

        if result.returncode != 0:
            print(f"[WARN] ffprobe falhou em {video_path.name}: {result.stderr.strip()}")
            return False

        keyframe_times = []

        for line in result.stdout.strip().splitlines():
            parts = line.split(",")

            if len(parts) != 3:
                continue

            key_frame, pts_time, pkt_pts_time = parts

            # Usa o que estiver disponível entre os dois nomes
            time_value = pts_time if pts_time not in ("", "N/A") else pkt_pts_time

            if key_frame == "1" and time_value not in ("", "N/A"):
                try:
                    keyframe_times.append(float(time_value))
                except ValueError:
                    continue

        if not keyframe_times:
            print(f"[WARN] Nenhum keyframe encontrado em {video_path.name}")
            return False

        gaps = [
            b - a
            for a, b in zip(keyframe_times, keyframe_times[1:])
        ]

        max_found_gap = max(gaps) if gaps else 0.0

        if max_found_gap > max_gap:
            print(
                f"[WARN] GOP muito longo em {video_path.name}: "
                f"{max_found_gap:.1f}s (máx permitido: {max_gap}s)"
            )
            return False

        return True

    except subprocess.TimeoutExpired:
        print(f"[WARN] ffprobe demorou demais em {video_path.name}")
        return False

    except Exception as e:
        print(f"[WARN] Erro ao validar GOP de {video_path.name}: {e}")
        return False
def quarantine_video(video_path):
    """
    Renomeia o vídeo com prefixo _encode_ na mesma pasta
    (pasta de duplicados), pra não ser pego de novo pelo
    get_videos() e ficar visível pra quem for reencodar depois.
    """

    new_name = video_path.parent / f"_encode_{video_path.name}"

    if new_name.exists():
        return

    try:
        video_path.rename(new_name)
        print(f"[QUARANTINE] Renomeado para: {new_name.name}")

    except Exception as e:
        print(f"[ERROR] Não foi possível renomear {video_path.name}: {e}")

# ============================================================
# VIDEO DISCOVERY (substitui a função atual)
# ============================================================

def is_file_stable(video_path, min_age=MIN_FILE_AGE_SECONDS):
    """
    Considera o arquivo estável se ele não foi modificado nos
    últimos `min_age` segundos, usando mtime do próprio disco.
    Isso funciona mesmo logo após um restart do container,
    diferente de comparar duas leituras em memória.
    """

    try:
        mtime = video_path.stat().st_mtime
    except FileNotFoundError:
        return False

    age = time.time() - mtime

    return age >= min_age


# ============================================================
# VIDEO DISCOVERY (substitui a versão anterior)
# ============================================================

# ============================================================
# ORDENAÇÃO DA PLAYLIST
# ============================================================

def order_videos(videos):
    """
    Ordena a lista de vídeos de acordo com ORDER_VIDEO_FEED.
    """

    if ORDER_VIDEO_FEED == "random":
        import random
        shuffled = videos.copy()
        random.shuffle(shuffled)
        return shuffled

    if ORDER_VIDEO_FEED == "newest":
        return sorted(videos, key=lambda p: p.stat().st_mtime, reverse=True)

    if ORDER_VIDEO_FEED == "oldest":
        return sorted(videos, key=lambda p: p.stat().st_mtime)

    # alphabetic (padrão)
    return sorted(videos, key=lambda p: p.name)

def get_videos():

    if not VIDEO_DIR.exists():
        print(f"[ERROR] Pasta não existe: {VIDEO_DIR}")
        return []

    candidates = [
        path
        for path in VIDEO_DIR.iterdir()
        if (
            path.is_file()
            and path.name.lower().endswith(VIDEO_SUFFIX)
            and not path.name.startswith("_encode_")
        )
    ]

    candidates = order_videos(candidates)   # <-- troca aqui

    if not CHECKS_FILE_READY:
        return candidates

    valid_videos = []

    for video in candidates:

        if not is_file_stable(video):
            print(f"[SKIP] Arquivo modificado recentemente: {video.name}")
            continue

        key = str(video)
        cached = _file_state_cache.get(key)

        if cached is None:
            if is_valid_gop(video):
                _file_state_cache[key] = {"gop_ok": True}
            else:
                quarantine_video(video)
                continue
            cached = _file_state_cache[key]

        if cached["gop_ok"]:
            valid_videos.append(video)

    return valid_videos

# ============================================================
# METADADOS DO NOME DO ARQUIVO
# ============================================================

# Ex: "BRUNO LEMELA - EX-PRODUTOR DO PÂNICO... #ep57 -  ELENCO B CAST - Elenco B Cast - 2026-08-23_5_small_cut_subtitles.mp4"
#
# Estratégia: separar pelo FINAL do nome, que é fixo e confiável
# (CHANNEL_UPPER - Channel Display - date_id_suffix). O que sobrar
# no começo é o título, mesmo que tenha " - " dentro dele.

EPISODE_PATTERN = re.compile(r"#ep(\d+)", re.IGNORECASE)


def parse_video_metadata(video_path):
    """
    Extrai título, canal, episódio e data do nome do arquivo.
    Se o nome não bater com o padrão esperado, cai pra um
    fallback (só o nome do arquivo sem extensão).
    """

    name = video_path.name

    if not name.lower().endswith(VIDEO_SUFFIX.lower()):
        return {"title": video_path.stem, "channel": "", "episode": "", "date": ""}

    base = name[: -len(VIDEO_SUFFIX)]  # remove sufixo fixo

    # Separa "..._5" (id) do final -> data
    date_match = re.search(r"-\s*(\d{4}-\d{2}-\d{2})_\d+$", base)
    date = ""
    if date_match:
        date = date_match.group(1)
        base = base[: date_match.start()].rstrip(" -")

    # Agora 'base' termina em "... - CHANNEL_UPPER - Channel Display"
    parts = base.split(" - ")
    channel = parts[-1].strip() if parts else ""

    # descarta o penúltimo pedaço (versão uppercase do canal), se existir
    if len(parts) >= 3:
        title = " - ".join(parts[:-2]).strip()
    elif len(parts) > 1:
        title = " - ".join(parts[:-1]).strip()
    else:
        title = base.strip()

    episode_match = EPISODE_PATTERN.search(title)
    episode = f"EP{episode_match.group(1)}" if episode_match else ""

    # Remove o "#ep57" do título pra não duplicar na tela
    title = EPISODE_PATTERN.sub("", title).strip(" -")

    return {
        "title": title,
        "channel": channel,
        "episode": episode,
        "date": date,
    }


# ============================================================
# BUMPER (IMAGEM ESTÁTICA -> CLIPE CURTO, INTRO = OUTRO)
# ============================================================

# Default: subpasta dentro de VIDEO_DIR (não /tmp), porque /tmp
# costuma NÃO ser persistido entre restarts do container — o que
# faria o cache sumir e todo vídeo travar de novo na próxima subida.
# VIDEO_DIR já é o volume persistente, então o cache sobrevive.
BUMPER_DIR = Path(os.getenv("BUMPER_DIR", str(VIDEO_DIR / ".bumpers")))
BUMPER_DURATION = float(os.getenv("BUMPER_DURATION", "5"))
BUMPER_ENABLED = os.getenv("BUMPER_ENABLED", "true").lower() not in ("false", "0", "")

# Resolução/fps do bumper. Se BUMPER_WIDTH/BUMPER_HEIGHT não forem
# definidos no .env, detectamos automaticamente a partir do primeiro
# vídeo real encontrado (evita trocar de resolução no meio da live,
# o que o YouTube não gosta e pode gerar glitch/erro no encoder).
BUMPER_WIDTH_ENV = os.getenv("BUMPER_WIDTH")
BUMPER_HEIGHT_ENV = os.getenv("BUMPER_HEIGHT")
BUMPER_FPS = os.getenv("BUMPER_FPS", "30")

# Fallback caso a detecção automática falhe e nada esteja no .env
_FALLBACK_WIDTH, _FALLBACK_HEIGHT = 1920, 1080

BUMPER_WIDTH = int(BUMPER_WIDTH_ENV) if BUMPER_WIDTH_ENV else None
BUMPER_HEIGHT = int(BUMPER_HEIGHT_ENV) if BUMPER_HEIGHT_ENV else None


def detect_video_resolution(video_path):
    """
    Usa ffprobe pra ler width/height do primeiro stream de vídeo.
    Retorna (width, height) ou None se falhar.
    """

    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=width,height",
                "-of", "csv=s=x:p=0",
                str(video_path),
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )

        if result.returncode != 0:
            return None

        output = result.stdout.strip()
        if "x" not in output:
            return None

        width_str, height_str = output.split("x")
        return int(width_str), int(height_str)

    except Exception as e:
        print(f"[WARN] Não foi possível detectar resolução de {video_path.name}: {e}")
        return None


def ensure_bumper_resolution():
    """
    Garante que BUMPER_WIDTH/BUMPER_HEIGHT estejam definidos antes
    do primeiro bumper ser gerado. Se não vieram do .env, detecta
    a partir do primeiro vídeo disponível na pasta. Só roda uma vez.
    """

    global BUMPER_WIDTH, BUMPER_HEIGHT

    if BUMPER_WIDTH and BUMPER_HEIGHT:
        return  # já veio do .env, respeita a configuração manual

    videos = [
        p for p in VIDEO_DIR.iterdir()
        if p.is_file()
        and p.name.lower().endswith(VIDEO_SUFFIX)
        and not p.name.startswith("_encode_")
    ] if VIDEO_DIR.exists() else []

    resolution = None
    if videos:
        resolution = detect_video_resolution(videos[0])

    if resolution:
        BUMPER_WIDTH, BUMPER_HEIGHT = resolution
        print(f"[BUMPER] Resolução detectada automaticamente: {BUMPER_WIDTH}x{BUMPER_HEIGHT}")
    else:
        BUMPER_WIDTH, BUMPER_HEIGHT = _FALLBACK_WIDTH, _FALLBACK_HEIGHT
        print(
            f"[WARN] Não foi possível detectar resolução dos vídeos. "
            f"Usando fallback {BUMPER_WIDTH}x{BUMPER_HEIGHT} "
            f"— defina BUMPER_WIDTH/BUMPER_HEIGHT no .env se isso não bater "
            f"com seus vídeos."
        )

FONT_PATH = os.getenv("OVERLAY_FONT_PATH", "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf")
FONT_PATH_BOLD = os.getenv("OVERLAY_FONT_PATH_BOLD", "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf")

BUMPER_BADGE_TEXT = os.getenv("BUMPER_BADGE_TEXT", "BY PODCUT AGORA")

BUMPER_DIR.mkdir(parents=True, exist_ok=True)


def bumper_cache_key(video_path):
    """
    Chave de cache baseada no nome do arquivo (não no conteúdo,
    já que é o metadado no nome que muda o texto). Como o bumper
    é o mesmo pro intro e pro outro, só existe UM arquivo por vídeo.
    """

    h = hashlib.sha1(video_path.name.encode("utf-8")).hexdigest()[:16]
    return BUMPER_DIR / f"bumper_{h}.ts"


# Todas as medidas abaixo (fontes, espaçamentos, padding) foram
# calibradas pra uma referência de 1920x1080. O fator de escala
# converte isso proporcionalmente pra qualquer resolução real do
# bumper, evitando texto gigante em vídeos menores (ex: 640x360).
_REFERENCE_WIDTH = 1920


def render_bumper_image(meta, out_path):
    """
    Desenha a imagem estática do bumper com PIL: fundo escuro,
    faixa/badge no topo, título centralizado e canal/episódio/data
    embaixo. Todos os tamanhos escalam com BUMPER_WIDTH.
    """

    scale = BUMPER_WIDTH / _REFERENCE_WIDTH

    def s(value):
        """Escala um valor de referência (1920px) e garante mínimo de 1."""
        return max(1, round(value * scale))

    img = Image.new("RGB", (BUMPER_WIDTH, BUMPER_HEIGHT), color=(15, 15, 18))
    draw = ImageDraw.Draw(img)

    # Faixa de destaque no topo
    draw.rectangle([0, 0, BUMPER_WIDTH, s(8)], fill=(200, 30, 30))

    font_title = ImageFont.truetype(FONT_PATH_BOLD, s(58))
    font_subtitle = ImageFont.truetype(FONT_PATH, s(34))
    font_badge = ImageFont.truetype(FONT_PATH_BOLD, s(30))

    title = meta["title"]
    subtitle_parts = [p for p in [meta["channel"], meta["episode"], meta["date"]] if p]
    subtitle = "   •   ".join(subtitle_parts)

    # Quebra o título em linhas se for muito largo
    max_width = BUMPER_WIDTH - s(240)
    words = title.split()
    lines, current = [], ""

    for word in words:
        test = f"{current} {word}".strip()
        if draw.textlength(test, font=font_title) <= max_width:
            current = test
        else:
            lines.append(current)
            current = word
    if current:
        lines.append(current)

    lines = lines[:4]  # limita a 4 linhas pra não estourar a tela

    line_height = s(74)
    total_height = len(lines) * line_height + s(70)
    y = (BUMPER_HEIGHT - total_height) // 2

    for line in lines:
        w = draw.textlength(line, font=font_title)
        draw.text(((BUMPER_WIDTH - w) / 2, y), line, font=font_title, fill="white")
        y += line_height

    y += s(25)
    w = draw.textlength(subtitle, font=font_subtitle)
    draw.text(((BUMPER_WIDTH - w) / 2, y), subtitle, font=font_subtitle, fill=(190, 190, 190))

    # Badge no topo
    bw = draw.textlength(BUMPER_BADGE_TEXT, font=font_badge)
    bx, by = (BUMPER_WIDTH - bw) / 2, s(70)
    pad = s(20)
    draw.rectangle([bx - pad, by - s(10), bx + bw + pad, by + s(40)], fill=(200, 30, 30))
    draw.text((bx, by - s(2)), BUMPER_BADGE_TEXT, font=font_badge, fill="white")

    img.save(out_path)


def generate_bumper(video_path):
    """
    Gera (ou reusa do cache) o clipe de N segundos a partir de
    uma imagem estática. Encode acontece só na primeira vez que
    o vídeo aparece — depois disso é sempre reuso do cache.
    """

    cache_path = bumper_cache_key(video_path)

    if cache_path.exists():
        return cache_path

    meta = parse_video_metadata(video_path)

    tmp_image = BUMPER_DIR / f"_tmp_{cache_path.stem}.png"
    render_bumper_image(meta, tmp_image)

    command = [
        "ffmpeg", "-y",
        "-loop", "1",
        "-i", str(tmp_image),
        "-f", "lavfi",
        "-i", "anullsrc=r=44100:cl=stereo",
        "-t", str(BUMPER_DURATION),
        "-r", BUMPER_FPS,
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-crf", "20",
        "-pix_fmt", "yuv420p",
        "-g", "30",
        "-c:a", "aac",
        "-b:a", "128k",
        "-f", "mpegts",
        "-mpegts_flags", "+resend_headers",
        str(cache_path),
    ]

    print(f"[BUMPER] Gerando bumper para: {video_path.name}")

    result = subprocess.run(command, capture_output=True, text=True)
    tmp_image.unlink(missing_ok=True)

    if result.returncode != 0:
        print(f"[ERROR] Falha ao gerar bumper: {result.stderr.strip()}")
        return None

    return cache_path


def feed_ts_segment(ts_path):
    """
    Envia um .ts já pronto (bumper) direto pra FIFO, sem
    recodificar de novo — é só concatenação via copy.
    """

    command = [
        "ffmpeg", "-y",
        "-re",
        "-i", str(ts_path),
        "-c", "copy",
        "-f", "mpegts",
        "-mpegts_flags", "+resend_headers",
        str(FIFO_PATH),
    ]

    return subprocess.run(command)


# ============================================================
# PREFETCH DE BUMPERS EM BACKGROUND
# ============================================================
#
# Gerar o bumper é um encode (alguns segundos). Se isso rodar só
# na hora de tocar o vídeo, trava a live enquanto gera. Solução:
# assim que o feeder começa a tocar o vídeo atual (-c copy,
# streaming em tempo real e demorado por natureza), disparamos em
# paralelo a geração do bumper do PRÓXIMO vídeo numa thread
# separada. Quando chegar a vez dele, o bumper já está pronto (ou
# quase) e o "feed_ts_segment" nem percebe atraso.
#
# IMPORTANTE: get_bumper() NUNCA bloqueia a live esperando geração
# terminar. Se não estiver pronto na hora (ex: vídeo tocou mais
# rápido que a geração, ou muitos vídeos novos em sequência
# enchendo a fila do worker único), o vídeo simplesmente toca sem
# bumper dessa vez — sem soluço, sem silêncio na FIFO. Na próxima
# vez que esse vídeo aparecer na playlist, o bumper já vai estar
# pronto. Complementa isso a bumper_warmup_loop() abaixo, que gera
# em background, continuamente, os bumpers que ainda faltam —
# tanto os do catálogo já existente no boot quanto os de vídeos
# novos adicionados depois — sem nunca interferir na FIFO.

_bumper_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
_bumper_futures = {}  # str(video_path) -> Future


def prefetch_bumper(video_path):
    """
    Dispara a geração do bumper em background, se ainda não
    existir em cache e não estiver em andamento. Não bloqueia.
    """

    if not BUMPER_ENABLED or video_path is None:
        return

    if bumper_cache_key(video_path).exists():
        return  # já tem, nada a fazer

    key = str(video_path)
    existing = _bumper_futures.get(key)

    if existing is not None and not existing.done():
        return  # já está sendo gerado

    _bumper_futures[key] = _bumper_executor.submit(generate_bumper, video_path)


def bumper_warmup_loop():
    """
    Roda em thread separada, continuamente, em background. Varre
    a pasta de vídeos e garante (via prefetch_bumper) que todo
    vídeo tenha seu bumper gerado — mesmo os que nunca chegaram a
    tocar ainda. Cobre tanto o "aquecimento" inicial do catálogo
    inteiro no boot quanto vídeos novos adicionados depois, sem
    NUNCA bloquear o feeder/publisher: cada geração passa pela
    mesma fila (executor de 1 worker) usada pelo prefetch normal.
    """

    while running:
        try:
            videos = get_videos()

            for video in videos:
                if not running:
                    break
                prefetch_bumper(video)

                # Pequena pausa entre submissões pra não empilhar
                # dezenas de vídeos de uma vez só no arranque —
                # o executor processa um por vez de qualquer forma,
                # isso só evita gastar tempo escaneando à toa.
                interruptible_sleep(0.5)

        except Exception as e:
            print(f"[WARN] Erro no warmup de bumpers: {e}")

        interruptible_sleep(30)  # revisita periodicamente por vídeos novos


def get_bumper(video_path):
    """
    Retorna o caminho do bumper SE já estiver pronto em cache.
    NUNCA bloqueia a live esperando geração: se ainda não existe
    (nem foi gerado, nem terminou de gerar em background), apenas
    dispara/garante o prefetch pra próxima vez e retorna None —
    o vídeo toca sem bumper só dessa vez. Na próxima passagem
    desse mesmo vídeo na playlist, o bumper já vai estar pronto.
    """

    if not BUMPER_ENABLED:
        return None

    cache_path = bumper_cache_key(video_path)

    if cache_path.exists():
        return cache_path

    prefetch_bumper(video_path)  # garante que entra na fila pra próxima vez
    return None


# ============================================================
# PUBLISHER — conexão RTMP única e persistente
# ============================================================

def start_publisher():
    """
    Único responsável por falar com o YouTube. Fica lendo da
    FIFO continuamente. Só reinicia se a conexão RTMP em si
    cair (erro de rede) — nunca por causa de troca de vídeo.
    """

    if not YOUTUBE_STREAM_KEY:
        raise RuntimeError("YOUTUBE_STREAM_KEY não configurada.")

    stream_url = f"{YOUTUBE_RTMP_URL}/{YOUTUBE_STREAM_KEY}"

    command = [
        "ffmpeg",
        "-i", str(FIFO_PATH),
        "-c", "copy",
        "-f", "flv",
        stream_url,
    ]

    print()
    print("=" * 60)
    print("[PUBLISHER] Abrindo conexão RTMP persistente com YouTube")
    print("=" * 60)

    return subprocess.Popen(command)


# ============================================================
# FEEDER — alimenta a FIFO, vídeo por vídeo
# ============================================================


def feed_video(video_path):
    """
    Reempacota o vídeo em MPEG-TS (sem recodificar) e escreve
    na FIFO respeitando tempo real (-re). Como o publisher já
    está com a conexão aberta, isso NÃO gera reconexão no
    YouTube — é só mais dado chegando no mesmo stream.
    """

    command = [
        "ffmpeg",
        "-y",              # <-- não perguntar, sobrescrever/escrever direto na FIFO
        "-re",
        "-i", str(video_path),
        "-c", "copy",
        "-f", "mpegts",
        "-mpegts_flags", "+resend_headers",
        str(FIFO_PATH),
    ]

    print(f"[FEEDER] Enviando: {video_path.name}")

    return subprocess.run(command)


def feed_video_with_bumpers(video_path, next_video_path=None):
    """
    Envolve feed_video com o bumper (intro + outro, mesma imagem)
    quando habilitado. O vídeo original nunca é alterado nem
    recodificado — só o bumper (gerado uma vez e cacheado) passa
    por encode.

    Se next_video_path for informado, dispara em background a
    geração do bumper do PRÓXIMO vídeo assim que o atual começa
    a tocar — assim, quando chegar a vez dele, o bumper já está
    pronto e não trava a live.
    """

    bumper = get_bumper(video_path) if BUMPER_ENABLED else None

    if bumper:
        set_now_playing("bumper", video_path)
        feed_ts_segment(bumper)
    elif BUMPER_ENABLED:
        print(
            f"[BUMPER] Ainda não pronto para {video_path.name} — "
            f"tocando sem bumper desta vez (gerando em background pra próxima)."
        )

    # Aproveita o tempo real que o vídeo atual leva pra tocar
    # (-re, streaming em tempo real) pra gerar o bumper do
    # próximo em paralelo, numa thread separada.
    if BUMPER_ENABLED and next_video_path is not None:
        prefetch_bumper(next_video_path)

    set_now_playing("video", video_path)
    result = feed_video(video_path)

    if bumper and result.returncode == 0:
        set_now_playing("bumper", video_path)
        feed_ts_segment(bumper)  # mesmo arquivo já gerado, sem custo extra

    return result


def feeder_loop():
    """
    Roda em thread separada, pra sempre. Percorre os vídeos em
    loop, escrevendo cada um na FIFO. Se escrever e não tiver
    ninguém lendo (publisher caiu), o ffmpeg do feeder trava
    esperando um leitor aparecer de novo — assim que o publisher
    reconectar na mesma FIFO, o feeder volta a fluir sozinho.
    """

    while running:

        videos = get_videos()

        if not videos:
            print("[WAIT] Nenhum vídeo encontrado. Aguardando...")
            interruptible_sleep(10)
            continue

        print(f"[INFO] {len(videos)} vídeo(s) encontrado(s).")

        for index, video in enumerate(videos):

            if not running:
                break

            # Próximo da lista atual; se for o último, não há
            # como saber com certeza qual será o próximo (a ordem
            # pode ser reembaralhada na próxima volta), então
            # simplesmente não faz prefetch — o pior caso é esse
            # único vídeo pagar o custo de geração bloqueante dessa vez.
            next_video = videos[index + 1] if index + 1 < len(videos) else None

            result = feed_video_with_bumpers(video, next_video)

            if not running:
                break

            if result.returncode == 0:
                print(f"[OK] Vídeo terminou: {video.name}")
            else:
                print(
                    f"[ERROR] Feeder terminou com código "
                    f"{result.returncode} em {video.name}"
                )
                interruptible_sleep(RESTART_DELAY)

        if running:
            print("[LOOP] Todos os vídeos foram reproduzidos. Reiniciando...")


# ============================================================
# WEB — FastAPI com a rota "/" (lista de vídeos + tocando agora)
# ============================================================
#
# Servido em lives.f5sites.com (via proxy reverso apontando pra
# essa porta). Roda em thread separada, sem interferir no feeder
# nem no publisher.

API_HOST = os.getenv("API_HOST", "lives.f5sites.com")
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
    now_playing = get_now_playing()
    all_videos = sorted(
        (
            p for p in VIDEO_DIR.iterdir()
            if VIDEO_DIR.exists()
            and p.is_file()
            and p.name.lower().endswith(VIDEO_SUFFIX)
            and not p.name.startswith("_encode_")
        ) if VIDEO_DIR.exists() else [],
        key=lambda p: p.name,
    )

    current_path = now_playing["video_path"]
    current_kind = now_playing["kind"]
    elapsed = _format_elapsed(now_playing["started_at"])

    # ---- bloco "tocando agora" ----
    if current_path is not None:
        meta = parse_video_metadata(current_path)
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
        meta = parse_video_metadata(video)
        is_current = current_path is not None and video == current_path
        has_bumper = bumper_cache_key(video).exists() if BUMPER_ENABLED else None

        bumper_icon = ""
        if BUMPER_ENABLED:
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
        '<tr><td colspan="6">Nenhum vídeo encontrado em ' + _escape_html(str(VIDEO_DIR)) + '</td></tr>'
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
    print(f"[API] Servindo em http://{API_HOST}:{API_PORT}/")
    uvicorn.run(app, host=API_HOST, port=API_PORT, log_level="warning")


# ============================================================
# MAIN
# ============================================================

def main():
    global publisher_process

    print("=" * 60)
    print("YouTube 24/7 Streamer (RTMP persistente)")
    print("=" * 60)

    print(f"[CONFIG] Video directory: {VIDEO_DIR}")
    print(f"[CONFIG] Video suffix:    {VIDEO_SUFFIX}")
    print(f"[CONFIG] FIFO:            {FIFO_PATH}")
    print(f"[CONFIG] Bumper enabled:  {BUMPER_ENABLED}")
    print(f"[CONFIG] API:             http://{API_HOST}:{API_PORT}/")
    print()

    ensure_fifo()

    if BUMPER_ENABLED:
        ensure_bumper_resolution()

    # Feeder roda em background, independente do ciclo de vida
    # do publisher.
    feeder_thread = threading.Thread(target=feeder_loop, daemon=True)
    feeder_thread.start()

    # Warmup roda em background, continuamente, gerando bumpers
    # que ainda faltam -- nunca bloqueia a live.
    if BUMPER_ENABLED:
        warmup_thread = threading.Thread(target=bumper_warmup_loop, daemon=True)
        warmup_thread.start()

    # API web (lista de vídeos + tocando agora), também em background.
    api_thread = threading.Thread(target=start_api_server, daemon=True)
    api_thread.start()

    while running:

        try:
            publisher_process = start_publisher()

        except Exception as e:
            print(f"[ERROR] Não foi possível iniciar publisher: {e}")
            interruptible_sleep(RESTART_DELAY)
            continue

        return_code = publisher_process.wait()
        publisher_process = None

        if not running:
            break

        print(
            f"[ERROR] Publisher caiu (código {return_code}). "
            f"Reconectando em {RESTART_DELAY}s..."
        )
        interruptible_sleep(RESTART_DELAY)

    print("[SYSTEM] Streamer encerrado.")


if __name__ == "__main__":
    main()