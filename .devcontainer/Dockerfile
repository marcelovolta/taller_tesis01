FROM mcr.microsoft.com/devcontainers/python:1-3.12-bookworm

RUN apt-get update \
    && apt-get install -y postgresql-client \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip \
    && pip install "dbt-postgres==1.9.0"

# Basic libraries for data manipulation
RUN pip install "pandas==2.2.3" \
    && pip install "polars==1.23.0" \
    && pip install "duckdb==1.5.2"

# Needed for getting comments from YouTube
RUN pip install "google-api-python-client==2.182.0"
