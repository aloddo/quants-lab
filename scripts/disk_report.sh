#!/bin/bash
# Monthly disk report (Step E of 2026-07-02 disk-cleanup plan). Run manually or via CoS cron.
echo "=== DISK REPORT $(date +%F) ==="
df -h /System/Volumes/Data | awk 'NR==2{print "Data volume: "$3" used / "$2" ("$5")  free "$4}'
cd /Users/hermes/quants-lab
echo "--- app/data top-level (raw is sacred; caches purgeable) ---"
du -sh app/data/* 2>/dev/null | sort -rh | head -12
echo "--- naming violations (should be EMPTY) ---"
find app/data -maxdepth 2 \( -name "*.bak" -o -name "*_old*" -o -name "prefix_backup_*" \) 2>/dev/null | head
echo "--- mongo ---"
mongosh --quiet mongodb://localhost:27017/quants_lab --eval 'const s=db.stats(); print("storage "+(s.storageSize/1e9).toFixed(1)+"G data "+(s.dataSize/1e9).toFixed(1)+"G collections "+s.collections)' 2>/dev/null
