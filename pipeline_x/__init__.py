from dagster import schedule, Definitions, EnvVar, load_assets_from_modules
from dagster_aws.s3 import S3Resource, S3PickleIOManager
from dagster_docker import docker_executor
from .job import assets, assets_1
from .job.assets import close_data_pipeline
from .job.assets_1 import close_data_pipeline_1
from .resource.sql_alchemy import SqlAlchemyClientResource


all_assets = load_assets_from_modules([assets])
all_assets_1 = load_assets_from_modules([assets_1])


# Define jobs from assets
#close_pipeline = close_data_pipeline.to_job(name="etl_pipeline_x", executor_def=docker_executor)
#all_assets = load_assets_from_modules([assets])

@schedule(cron_schedule="0 8 * * *", job=close_data_pipeline)
def schedule_etl_pipeline(_context):
    return {}

@schedule(cron_schedule="0 9 * * *", job=close_data_pipeline_1)
def schedule_etl_pipeline_1(_context):
    return {}

defs = Definitions(
    assets=[*all_assets,*all_assets_1],
    jobs=[close_data_pipeline,close_data_pipeline_1],
    schedules=[schedule_etl_pipeline, schedule_etl_pipeline_1]
)

# defs = Definitions(
#     resources={
#         "io_manager": S3PickleIOManager(
#             s3_resource=S3Resource(aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
#                                    aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY")),
#             s3_bucket="etl-dagster-data",
#             s3_prefix="op-io-data",
#         ),
#         "s3_io_manager": S3Resource(aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
#                                     aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY")),
#         "postgres_io_manager": SqlAlchemyClientResource(database_ip=EnvVar("DATABASE_IP"),
#                                                         database_port=EnvVar("DATABASE_PORT"),
#                                                         database_user=EnvVar("DATABASE_USER"),
#                                                         database_password=EnvVar("DATABASE_PASSWORD"),
#                                                         database_name=EnvVar("DATABASE_NAME")
#                                                         )
#     },
#     schedules=[schedule_etl])
