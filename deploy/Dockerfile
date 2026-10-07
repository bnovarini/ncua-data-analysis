FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .
# Bake the release data into the image so the server never downloads at runtime.
ENV NCUA_DATA_DIR=/data
RUN python -m ncua_data.mcp_server --check
ENV NCUA_ALLOWED_HOSTS=ncua-data-analysis.fly.dev NCUA_MEMORY_LIMIT=300MB NCUA_THREADS=1 NCUA_QUERY_TIMEOUT=10 NCUA_RATE_PER_MIN=60
USER nobody
EXPOSE 8080
CMD ["python", "-m", "ncua_data.mcp_server", "--http"]
