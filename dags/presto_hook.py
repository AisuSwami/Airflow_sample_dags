from airflow.models import Variable
from airflow.hooks.base import BaseHook
import prestodb
import json


class PrestoHook(BaseHook):

    def __init__(
            self,
            presto_host,
            presto_port,
            presto_catalogue,
            schema,
            presto_service_account_file

    ) -> None:
        super().__init__()
        self.presto_host = presto_host
        self.presto_port = presto_port
        self.presto_catalogue = presto_catalogue
        self.schema = schema
        self.presto_service_account_file = presto_service_account_file

    def get_conn(self):
        def get_presto_username():
            with open(self.presto_service_account_file, "r") as read_file:         
                data = json.load(read_file)  
                return data["username"]

        def get_presto_password():
            with open(self.presto_service_account_file, "r") as read_file:
                data = json.load(read_file)
                return data["password"]

        return prestodb.dbapi.connect(
            host=self.presto_host,
            port=self.presto_port,
            user=get_presto_username(),
            catalog=self.presto_catalogue,
            schema=self.schema,
            http_scheme='https',
            auth=prestodb.auth.BasicAuthentication(
                get_presto_username(), get_presto_password())
        )

    def get_presto_username(self):
            with open(self.presto_service_account_file, "r") as read_file:

            # with open(json_path, "r") as read_file:
                data = json.load(read_file)
                print(data)
