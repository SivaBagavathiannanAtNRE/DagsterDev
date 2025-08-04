import pandas as pd
import boto3
import dagster
from dagster import op, graph, job
import json
from datetime import datetime, timedelta, date
from dateutil import parser
from closeio_api import Client
from typing import Tuple, Dict, List
from dotenv import load_dotenv
import os

logger = dagster.get_dagster_logger()
load_dotenv(dotenv_path=".env.local") 


@dagster.asset
def get_close_api_key_1():
    secret_name = "close/subto"
    region_name = "us-east-1"
    logger.info("This is an info message from Dagster logger.")

    session = boto3.Session(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION') 
    )
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    close_api_key = client.get_secret_value(
            SecretId='close/brands'
        )
    close_customfields_key = client.get_secret_value(
            SecretId='close/initial_lead_status'
        )
    logger.info("Received credentials from AWS Secrets Manager")
    logger.info("Secret ID: close/brands")
    logger.info("Secret ID: close/initial_lead_status")
    logger.info("Secret ID: close/subto")
    d = json.loads(close_api_key['SecretString'])

    cf=json.loads(close_customfields_key['SecretString'])
    return cf,d


@dagster.asset(deps=[ get_close_api_key_1 ])
def get_close_data_1(get_close_api_key_1:Tuple[Dict, Dict]) ->  List[Dict]:
    yesterday = str(date.today() - timedelta(2))
    today = str(date.today() - timedelta(1))
    cf, d = get_close_api_key_1
    logger.info(cf)
    logger.info(d)
    api_key=d['SubTo']
    custom_field=cf.get('SubTo',None)
    logger.info("API Key")
    logger.info(api_key)
    logger.info("Custom Field")
    logger.info(custom_field)
    leads=[]
    api = Client(api_key)
    data={
        '_fields':{
            "lead": ['id','display_name','original_lead_status','status_label','date_created','date_updated','custom.'+custom_field,'custom.lcf_pm4lsEd6vjpvvvRuaHBayWkbIqtfYrRDxN6JlExsuQQ','custom.cf_AIb9wKkoPe968aakyl710oyIZHy1Nf7TO6Le9iIygoS']},
            "limit": None,
            "query": {
            "negate": False,
            "queries": [
                {
                    "negate": False,
                    "object_type": "lead",
                    "type": "object_type"
                },
                {
                    "negate": False,
                    "queries": [
                        {
                                        "condition": {
                                            "type": "term",
                                            "values": [
                                                "Booked Call",
                                                "Hot App",
                                                "Paid Hot App",
                                                "SDR Opp",
                                                "New SDR Opportunity",
                                                "New Opportunity",
                                                "New Hot App Opportunity",
                                                "New Booked Opportunity",
                                                "New PAID Hot App Oppportunity",
                                                "SDR New Opportunity"
                                            ]
                                        },
                                        "field": {
                                            "custom_field_id": custom_field,
                                            "type": "custom_field"
                                        },
                                        "negate": False,
                                        "type": "field_condition"
                                    },
                        {
                            "negate": False,
                            "queries": [
                                {
                                    "condition": {
                                        "before": {
                                            "type": "fixed_local_date",
                                            "value": today,
                                            "which": "end"
                                        },
                                        "on_or_after": {
                                            "type": "fixed_local_date",
                                            "value": yesterday,
                                            "which": "start"
                                        },
                                        "type": "moment_range"
                                    },
                                    "field": {
                                        "field_name": "date_created",
                                        "object_type": "lead",
                                        "type": "regular_field"
                                    },
                                    "negate": False,
                                    "type": "field_condition"
                                }
                            ],
                            "type": "and"
                        }
                    ],
                    "type": "and"
                }
            ],
            "type": "and"
        },
        "results_limit": None,
        "sort": []
        }
    
    logger.info(data)
    while "cursor" not in data or data["cursor"] != None:
        resp = api.post('data/search', data=data)
        logger.info(resp)
        logger.info(resp["cursor"])
        data["cursor"] = resp["cursor"]
        leads=resp["data"]
    return leads






@dagster.asset(deps=[ get_close_data_1 ])
def input_snowflake_data_1(get_close_data_1:List[Dict]) ->  pd.DataFrame:
    # Assuming you have a function to load data into Snowflake
    leads=get_close_data_1
    df=pd.DataFrame(leads)
    # load_data_to_snowflake(get_close_data)
    logger.info("Data loaded successfully")
    return df


@job
def close_data_pipeline_1():
    input_snowflake_data_1(get_close_data_1(get_close_api_key_1()))