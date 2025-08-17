# Dockerfile
FROM python:3.11-slim

# System deps (geopy/nominatim may need these)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates build-essential \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first (better layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Add app code
COPY . .

# Cloud Run listens on $PORT (we'll set 8080)
ENV PYTHONUNBUFFERED=1

EXPOSE 8080
CMD ["python", "-u", "TaxiToolBOT.py"]
