FROM python:3.13.13-slim-bookworm@sha256:eabbb62836ee44c18d350821e9f78488bcf65134bf763ae9989d63e611fa04d9

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*

COPY app/ /app/
