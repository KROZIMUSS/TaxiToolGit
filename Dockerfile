# syntax=docker/dockerfile:1
FROM python:3.11-slim

# Keep Python clean & chatty for logs
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# (Optional) system tools if wheels need building
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
  && rm -rf /var/lib/apt/lists/*

# Install deps first for better cache
COPY requirements.txt .
RUN pip install -r requirements.txt

# Add your code
COPY . .

# Start your bot
CMD ["python", "-u", "TaxiToolBOT.py"]
