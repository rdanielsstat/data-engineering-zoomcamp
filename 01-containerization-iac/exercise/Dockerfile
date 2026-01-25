FROM python:3.13.11-slim

# Copy uv binary from official uv image (multi-stage build pattern)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# Set working directory
WORKDIR /code

# Add virtual environment to PATH
ENV PATH="/code/.venv/bin:$PATH"

# Copy dependency files first (better layer caching)
COPY pyproject.toml uv.lock .python-version ./

# Install dependencies
RUN uv sync --locked

# Copy the ingestion script
COPY ingest_data.py .

# Set default command to run the ingestion
ENTRYPOINT ["python", "ingest_data.py"]