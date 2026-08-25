# Base oficial do Python. "slim" é o Debian mínimo -- pequeno,
# mas sem ffmpeg/fontes, que instalamos abaixo.
FROM python:3.12-slim

# ffmpeg: usado pra remux (-c copy) e pra gerar os bumpers.
# fonts-dejavu-core: fonte usada no desenho do bumper (Pillow).
# Sem isso, ImageFont.truetype() falha ao tentar abrir a fonte.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copia só o requirements primeiro pra aproveitar cache do Docker
# -- só reinstala as libs se requirements.txt mudar, não a cada
# alteração de código.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código da aplicação.
COPY live.py stream.py api.py ./

# Porta interna que o uvicorn escuta (mapeada no docker-compose
# como "8111:80" -- host:container).
EXPOSE 80

CMD ["python", "live.py"]