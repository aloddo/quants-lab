#!/usr/bin/env bash
# Wait for colima/Docker to be ready, then start the HB API stack.
# Called by LaunchDaemon com.quantslab.docker-stack or manually.
set -euo pipefail

COMPOSE_DIR="/Users/hermes/hummingbot/hummingbot-api"
PATCH_SCRIPT="/Users/hermes/quants-lab/scripts/patch_hb_docker.sh"
export DOCKER_HOST="unix:///Users/hermes/.colima/docker.sock"
export PATH="/opt/homebrew/bin:$PATH"

echo "$(date): Waiting for Docker daemon..."
for i in $(seq 1 60); do
    if docker info >/dev/null 2>&1; then
        echo "$(date): Docker daemon ready after ${i}s"
        break
    fi
    if [ "$i" -eq 60 ]; then
        echo "$(date): ERROR — Docker daemon not ready after 60s"
        exit 1
    fi
    sleep 1
done

echo "$(date): Starting HB API stack..."
cd "$COMPOSE_DIR"
docker compose up -d 2>&1

echo "$(date): Waiting for hummingbot-api container to be healthy..."
for i in $(seq 1 30); do
    if docker inspect --format='{{.State.Running}}' hummingbot-api 2>/dev/null | grep -q true; then
        echo "$(date): Container running after ${i}s"
        break
    fi
    sleep 1
done

echo "$(date): Applying Bybit demo patches..."
sleep 3  # let container fully initialize
bash "$PATCH_SCRIPT" 2>&1

echo "$(date): Restarting hummingbot-api with patches..."
docker restart hummingbot-api 2>&1

echo "$(date): Waiting for API to respond..."
for i in $(seq 1 30); do
    if curl -s -u admin:admin http://localhost:8000/ >/dev/null 2>&1; then
        echo "$(date): HB API ready after ${i}s"
        break
    fi
    sleep 1
done

echo "$(date): Docker stack startup complete."
