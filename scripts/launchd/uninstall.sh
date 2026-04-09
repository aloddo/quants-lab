#!/bin/bash
# Uninstall QuantsLab launchd services
set -e

LAUNCH_DIR="$HOME/Library/LaunchAgents"

for plist in "$LAUNCH_DIR"/com.quantslab.*.plist; do
    [ -f "$plist" ] || continue
    name=$(basename "$plist" .plist)
    launchctl bootout "gui/$(id -u)/$name" 2>/dev/null || true
    rm "$plist"
    echo "Removed: $name"
done

echo "QuantsLab services uninstalled."
echo "Log files preserved in ~/Library/Logs/quantslab/"
