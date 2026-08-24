#!/usr/bin/env python3

import os
import time
import signal
import subprocess
import threading
from pathlib import Path

import dotenv

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

        for video in videos:

            if not running:
                break

            result = feed_video(video)

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
    print()

    ensure_fifo()

    # Feeder roda em background, independente do ciclo de vida
    # do publisher.
    feeder_thread = threading.Thread(target=feeder_loop, daemon=True)
    feeder_thread.start()

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