# Use a lightweight and modern Python base image
FROM python:3.12-slim

# Set non-interactive mode for apt-get
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies:
# - gcc and python3-dev: Required to build tgcrypto wheel
# - ffmpeg: Sometimes required by yt-dlp for media operations
# - curl & unzip: Required to download and install Deno
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    ffmpeg \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Install Deno (Crucial for yt-dlp modern YouTube challenges)
RUN curl -fsSL https://deno.land/x/install/install.sh | sh
ENV DENO_INSTALL="/root/.deno"
ENV PATH="$DENO_INSTALL/bin:$PATH"

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application (main.py)
COPY . .

# Start the application
CMD ["python", "main.py"]
