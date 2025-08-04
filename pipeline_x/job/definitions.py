from dagster import Definitions, load_assets_from_modules
from . import assets
from Jobs.jobs import close_data_job, close_data_schedule

from Close_Data_pipeline import assets  # noqa: TID252

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    jobs=[close_data_job],
    schedules=[close_data_schedule]
)
