#!/usr/bin/env bash
set -euo pipefail

LOG=/var/log/piraweather_cron.log
touch "$LOG"

echo "=== PiraWeather pipeline starting ==="

# Run initial ingestion on startup — non-fatal so the cron daemon always starts
./run.sh --env prod || echo "WARNING: Initial run failed. Will retry on next cron schedule."

# Create test database from prod (dev only — set CREATE_TEST_DB=true to enable)
if [[ "${CREATE_TEST_DB:-false}" == "true" ]]; then
    python scripts/create_test_db.py || echo "WARNING: Could not create test database."
fi

# Set up daily cron job (runs at 06:00 UTC)
cat > /etc/cron.d/piraweather <<'CRONTAB'
0 6 * * * root cd /app && ./run.sh --env prod >> /var/log/piraweather_cron.log 2>&1
CRONTAB
chmod 0644 /etc/cron.d/piraweather

echo "=== Cron scheduled. Starting cron daemon. ==="

# Stream cron log to stdout so `docker logs pipeline` works
tail -f "$LOG" &

# Start cron in foreground (PID 1)
exec cron -f
