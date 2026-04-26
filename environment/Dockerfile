# Tag + digest for reproducible builds (refresh if digest/tag drift):
#   docker buildx imagetools inspect python:3.13-slim-bookworm
FROM python:3.13-slim-bookworm@sha256:56e35fafc0d8b712b438a6e22bc03eca23de22d412370c4678f6e549c74f2141

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*

COPY app/ /app/
