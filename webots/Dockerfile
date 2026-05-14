FROM cyberbotics/webots:R2025a-ubuntu22.04

RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends python3-pip && \
    rm -rf /var/lib/apt/lists/* && \
    pip3 install kafka-python-ng --quiet

COPY worlds/      /app/worlds/
COPY controllers/ /app/controllers/

WORKDIR /app

CMD ["webots", "--batch", "--no-rendering", "--mode=fast", "worlds/agrAI.wbt"]
