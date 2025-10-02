# Use an official slim Python base image
FROM python:3.10-slim AS base

# Install uv (static binary, very fast)
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates
ADD https://astral.sh/uv/install.sh /uv-installer.sh
RUN sh /uv-installer.sh && rm /uv-installer.sh
ENV PATH="/root/.local/bin/:$PATH"

# Set working directory
WORKDIR /app

# Copy project metadata (for dependency resolution)
COPY pyproject.toml uv.lock ./

# Install dependencies (system-wide in container)
RUN uv sync --frozen --no-dev --extra azure

# Copy the actual project code
COPY . .

# Run application (adjust entrypoint as needed)
CMD ["uv", "run", "python", "-m", "scripts.scraper"]
