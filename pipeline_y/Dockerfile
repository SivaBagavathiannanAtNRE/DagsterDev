FROM python:3.10-slim

# Install dagster dependencies
RUN pip install \
    dagster \
    dagster-postgres \
    dagster-docker \
    dagster-aws \
    pandas

# Set working directory
WORKDIR /opt/dagster/app

# Copy pipeline code (assumes build context is the root containing pipeline_x/)
COPY pipeline_y /opt/dagster/app/pipeline_y

# Ensure data dir exists
RUN mkdir -p /opt/dagster/app/data

# Expose gRPC port
EXPOSE 4047

# Start Dagster user code server (assumes pipeline_y is a module)
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4047", "-m", "pipeline_y"]
