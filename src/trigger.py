try:
    import unzip_requirements
except ImportError:
    pass

import json
import os
import logging

from datetime import date, timedelta
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import pandas as pd
import boto3

from src.main_db import DBInstance

load_dotenv()


def handler(event, context):
    try:
        download_files = Emblue().download_files()
    except Exception as e:
        logging.error(e)
    else:
        body = {
            "message": "Function executed successfully!",
            "download_files": download_files,
        }

        return {"statusCode": 200, "body": json.dumps(body)}


class Emblue:
    def __init__(
            self,
            starting_date=(date.today() - timedelta(days=7)),
            finishing_date=(date.today() - timedelta(days=1)),
    ):
        self.db_instance = DBInstance(public_key=os.getenv("CLIENT_KEY"))
        self.s3_client = boto3.client(
            service_name="s3",
            region_name=os.getenv("REGION"),
            aws_access_key_id=os.getenv("ACCESS_KEY"),
            aws_secret_access_key=os.getenv("SECRET_KEY"),
        )

        self.stf_client = boto3.client(
            service_name="stepfunctions",
            region_name=os.getenv("REGION"),
            aws_access_key_id=os.getenv("ACCESS_KEY"),
            aws_secret_access_key=os.getenv("SECRET_KEY"),
        )

        self.starting_date = starting_date.strftime("%Y%m%d")
        self.finishing_date = finishing_date.strftime("%Y%m%d")

    def __get_emblue_accounts(self):
        accounts = self.db_instance.handler(
            query="SELECT emb.hostname, emb.emblue_user, emb.password FROM em_blue as emb;"
        )
        return accounts

    def __get_date_range(self):
        return (
            pd.date_range(start=self.starting_date, end=self.finishing_date)
            .to_pydatetime()
            .tolist()
        )

    def download_files(self):
        started_events = []
        for date_file in self.__get_date_range():
            for account in self.__get_emblue_accounts():
                if self.__execute_event(account, date_file):
                    started_events.append({"date": date_file.strftime("%Y%m%d"), "account": account})
        return started_events

    def __execute_event(self, account, date_file):
        try:
            response = self.stf_client.start_execution(
                stateMachineArn=os.getenv("STATE_FUNCTION_ARN"),
                input=json.dumps(
                    {"account": account, "file_date": date_file.strftime("%Y%m%d")}
                ),
            )
        except ClientError as error:
            self.__write_log(account=account, error=error)
        else:
            return response

    def __write_log(self, account, error):
        self.db_instance.handler(query=f"""
            INSERT INTO em_blue_migration_log (date_migrated, account, status, message)
                VALUES (
                    '{date.today()}'
                    '{account[2]}',
                    'PENDING_TO_PROCESS',
                    '{str(error)}'
                );
            """
        )