FROM python:3.9-slim

WORKDIR /app

# Install system dependencies (e.g. for potential specific wheels, though slim usually ok)
# If using Pillow or others, might need some libs. For now just basic.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Create necessary directories
RUN mkdir -p src/sessions src/logs src/tmp

CMD ["python", "-m", "src.main"]
