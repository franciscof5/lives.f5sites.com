# Container STREAMER -- roda o ffmpeg de UMA live, sincronizando
# os vídeos de um bucket S3. Um container desses por canal/live.
# Reporta status pro container central (lives.f5sites.com).
FROM python:3.12-slim

# ffmpeg: remux (-c copy) e geração dos bumpers.
# fonts-dejavu-core: fonte usada no desenho do bumper (Pillow).
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY live.py ./

CMD ["python", "live.py"]