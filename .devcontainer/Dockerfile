FROM mcr.microsoft.com/devcontainers/python:1-3.12-bookworm

USER root


RUN pip install --upgrade pip \
    && pip install "dbt-postgres==1.9.0"

# Basic libraries for data manipulation
RUN pip install "pandas==2.2.3" \
    && pip install "polars==1.23.0" \
    && pip install "duckdb==1.5.2"

# Needed for getting comments from YouTube
RUN pip install "google-api-python-client==2.182.0"

# Needed for building the movie database
RUN pip install "requests" \
&& pip install "sqlalchemy" \
&& pip install "psycopg2-binary"

# Needed for secure retrieval of credentials
RUN pip install "pydantic==2.12.5" \
&& pip install "pydantic-settings==2.13.1" \
&& pip install "pydantic_core==2.41.5"

RUN pip install "python-dotenv==1.2.1" \
&& pip install "PyYAML==6.0.3" \
&& pip install "six==1.17.0" \
&& pip install "typing-inspection==0.4.2" \
&& pip install "typing_extensions==4.15.0"
