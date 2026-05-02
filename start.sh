#!/bin/bash
# ─────────────────────────────────────────────────────────────────
# AXANCTUM INTELLIGENCE 911 — Unified Start Script
# Jalankan dashboard server (server.py) + screener bot (main.py)
# secara bersamaan dalam satu Railway service
# ─────────────────────────────────────────────────────────────────

echo "▶ Starting AXANCTUM Dashboard Server..."
uvicorn server:app --host 0.0.0.0 --port ${PORT:-8000} &
SERVER_PID=$!

# Tunggu server siap sebelum screener mulai kirim sinyal
sleep 5

echo "▶ Starting AKSA Microstructure Screener..."
python main.py &
SCREENER_PID=$!

echo "✅ Both processes running | Server PID=$SERVER_PID | Screener PID=$SCREENER_PID"

# Kalau salah satu mati, kill yang lain juga (supaya Railway restart keduanya)
wait -n
echo "⚠️ One process exited — shutting down both"
kill $SERVER_PID $SCREENER_PID 2>/dev/null
