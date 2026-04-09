#!/bin/bash
# Install QuantsLab launchd services (auto-restart on crash/reboot)
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LAUNCH_DIR="$HOME/Library/LaunchAgents"
LOG_DIR="$HOME/Library/Logs/quantslab"

# Create log directory
mkdir -p "$LOG_DIR"

# Source .env into the plist environment (launchd doesn't read .env)
ENV_FILE="$SCRIPT_DIR/../../.env"
if [ -f "$ENV_FILE" ]; then
    echo "Loading environment from .env..."
    # Inject env vars into plists via PlistBuddy
    while IFS='=' read -r key value; do
        # Skip comments and empty lines
        [[ "$key" =~ ^#.*$ ]] && continue
        [[ -z "$key" ]] && continue
        # Strip quotes
        value="${value%\"}"
        value="${value#\"}"
        value="${value%\'}"
        value="${value#\'}"
        for plist in "$SCRIPT_DIR"/com.quantslab.*.plist; do
            /usr/libexec/PlistBuddy -c "Add :EnvironmentVariables:$key string $value" "$plist" 2>/dev/null || \
            /usr/libexec/PlistBuddy -c "Set :EnvironmentVariables:$key $value" "$plist" 2>/dev/null || true
        done
    done < "$ENV_FILE"
fi

# Copy plists
for plist in "$SCRIPT_DIR"/com.quantslab.*.plist; do
    name=$(basename "$plist")
    cp "$plist" "$LAUNCH_DIR/$name"
    echo "Installed: $LAUNCH_DIR/$name"
done

# Load services
for plist in "$LAUNCH_DIR"/com.quantslab.*.plist; do
    name=$(basename "$plist" .plist)
    # Unload first if already loaded
    launchctl bootout "gui/$(id -u)/$name" 2>/dev/null || true
    launchctl bootstrap "gui/$(id -u)" "$plist"
    echo "Started: $name"
done

echo ""
echo "QuantsLab services installed and started."
echo "Logs: $LOG_DIR/"
echo ""
echo "Check status: launchctl list | grep quantslab"
echo "Stop:    launchctl bootout gui/$(id -u)/com.quantslab.pipeline"
echo "Restart: launchctl kickstart -k gui/$(id -u)/com.quantslab.pipeline"
