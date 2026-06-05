#!/usr/bin/env bash
# Capture the architecture animation to a GIF.
#
# Requirements:
#   - node (>= 18)
#   - ffmpeg
# Puppeteer is installed on-demand into a local node_modules.
#
# Usage:
#   ./capture.sh                      # default: 15fps, 12s
#   FPS=20 DURATION=10 ./capture.sh   # override
set -euo pipefail

cd "$(dirname "$0")"

FPS="${FPS:-15}"
DURATION="${DURATION:-12}"
WIDTH="${WIDTH:-1200}"
HEIGHT="${HEIGHT:-720}"

if ! command -v node >/dev/null 2>&1; then
  echo "node is required (>= 18). Aborting." >&2
  exit 1
fi
if ! command -v ffmpeg >/dev/null 2>&1; then
  echo "ffmpeg is required. Install via 'brew install ffmpeg' (macOS)." >&2
  exit 1
fi

if [ ! -d node_modules/puppeteer ]; then
  echo "Installing puppeteer locally..."
  npm init -y >/dev/null
  npm pkg set type=module >/dev/null
  npm install --no-fund --no-audit puppeteer
fi

node capture.mjs --fps="$FPS" --duration="$DURATION" --width="$WIDTH" --height="$HEIGHT"

echo "Assembling architecture.gif ..."
ffmpeg -y -loglevel error \
  -framerate "$FPS" \
  -i frames/frame_%04d.png \
  -vf "fps=$FPS,split[s0][s1];[s0]palettegen=stats_mode=full[p];[s1][p]paletteuse=dither=sierra2_4a" \
  -loop 0 \
  architecture.gif

echo "✓ architecture.gif written ($(du -h architecture.gif | cut -f1))"
