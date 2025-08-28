FROM apache/airflow:2.9.2

# Install curl and procps for health checks
USER root
RUN apt-get update && \
    apt-get install -y curl procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy and install requirements
COPY ./config/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt || echo "No requirements.txt found"

# Install Scrapy dependencies for RSS extractor
COPY ./app/redfin_scraper/requirements.txt /tmp/scrapy-requirements.txt
RUN pip install --no-cache-dir -r /tmp/scrapy-requirements.txt || echo "No scrapy requirements.txt found"
