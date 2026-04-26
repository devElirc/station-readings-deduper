# Use the tag only so Docker resolves the correct arch from the multi-arch manifest.
# A single image digest (often amd64-only) causes "exec /bin/sh: exec format error" on
# other hosts (e.g. arm64). To pin reproducibly across platforms, use the manifest
# *index* digest from: docker buildx imagetools inspect python:3.13-slim-bookworm
FROM python:3.13-slim-bookworm

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*

COPY app/ /app/
