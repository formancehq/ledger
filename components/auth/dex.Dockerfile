FROM ghcr.io/dexidp/dex:v2.35.0
ENV DEX_FRONTEND_DIR=/app/web
COPY --chown=root:root pkg/web /app/web
