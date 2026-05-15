FROM cyberbotics/webots:R2025a-ubuntu22.04

# libxcb-cursor0 requis par Qt xcb depuis Qt 6.5
# xvfb + xauth : display virtuel avec authentification explicite
RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends \
        python3-pip \
        xvfb \
        xauth \
        libxcb-cursor0 && \
    rm -rf /var/lib/apt/lists/* && \
    pip3 install kafka-python-ng --quiet

COPY worlds/      /app/worlds/
COPY controllers/ /app/controllers/

WORKDIR /app

# Démarre Xvfb manuellement, génère le cookie MIT-MAGIC-COOKIE,
# puis lance Webots avec DISPLAY et XAUTHORITY corrects.
CMD ["/bin/sh", "-c", \
    "Xvfb :99 -screen 0 1280x1024x24 & \
     sleep 2 && \
     xauth add :99 . $(mcookie) && \
     DISPLAY=:99 XAUTHORITY=~/.Xauthority \
     webots --batch --no-rendering --mode=fast worlds/agrAI.wbt"]
