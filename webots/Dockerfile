FROM cyberbotics/webots:R2025a-ubuntu22.04

# libxcb-cursor0 requis par Qt xcb depuis Qt 6.5
# xvfb fournit un display virtuel nécessaire même en --no-rendering
RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends \
        python3-pip \
        xvfb \
        libxcb-cursor0 && \
    rm -rf /var/lib/apt/lists/* && \
    pip3 install kafka-python-ng --quiet

COPY worlds/      /app/worlds/
COPY controllers/ /app/controllers/

WORKDIR /app

CMD ["xvfb-run", "-a", "webots", "--batch", "--no-rendering", "--mode=fast", "worlds/agrAI.wbt"]
