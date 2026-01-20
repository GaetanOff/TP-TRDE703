FROM eclipse-temurin:11-jdk

# Install Python
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy application code
COPY etl/ ./etl/

# Run ETL pipeline
CMD ["python3", "-m", "etl.main"]
