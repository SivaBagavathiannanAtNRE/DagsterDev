FROM python:3.10-slim

# Install dagster dependencies
RUN pip install \
    dagster \
    dagster-postgres \
    dagster-docker \
    dagster-aws \
    pandas
# Copy requirements and install
COPY requirements.txt .

RUN pip install -r requirements.txt

# Set working directory
WORKDIR /opt/dagster/app

#COPY . /opt/dagster/app


# Copy pipeline code (assumes build context is the root containing pipeline_x/)
COPY pipeline_x /opt/dagster/app/pipeline_x

# Ensure data dir exists
RUN mkdir -p /opt/dagster/app/data

# Expose gRPC port
EXPOSE 4000

# Start Dagster user code server (assumes pipeline_x is a module)
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "pipeline_x"]
