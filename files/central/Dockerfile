# Container CENTRAL -- só o painel web (FastAPI). Não roda ffmpeg,
# não toca vídeo. Agrega o status que cada streamer reporta.
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py ./

EXPOSE 80

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "80"]
