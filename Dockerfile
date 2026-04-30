# ── Build stage ───────────────────────────────────────────────────────────────
FROM python:3.10-slim AS builder

WORKDIR /app

# Install dependencies into a prefix so we can copy them cleanly
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM python:3.10-slim

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application source
COPY app/        ./app/
COPY src/        ./src/
COPY data/       ./data/

# Non-root user for security
RUN useradd -m appuser && chown -R appuser /app
USER appuser

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    FLASK_APP=app/advanced_analysis_app.py

EXPOSE 5002

CMD ["python", "app/advanced_analysis_app.py"]
