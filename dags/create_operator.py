from airflow.models.baseoperator import BaseOperator
from airflow.models import Variable
import prestodb
import json
import logging


class PrestoDBOperator(BaseOperator):
    """
    This operator is to be used to connect to Presto Twilio instances and run your queries. It needs
    service_account for accessing presto which it expects to be present in a location which is to be passed
    through airflow variable. You have to pass the  variable name through the argument `presto_service_account_variable_key`.
    The twilio way to inject this file into your container/instance is to use TwilioSecrets. Out of scope for this documentation.
    `presto_pool` : It controls the no of tasks which can run in parallel for this operator. By default we use a presto_pool defined in
    airflow ui with a value of 5. This make sure we dont bombard presto with a lot of concurrent queries
    """
    template_fields = ['query']
    template_fields_renderers = {'query': 'sql'}

    def __init__(
            self,
            name: str,
            query: str,
            schema: str,
            presto_service_account_variable_key: str,
            no_of_columns: int = -1,
            no_of_rows: int = -1,
            **kwargs) -> None:
        if 'pool' not in kwargs:
            kwargs['pool'] = 'presto_pool'
        super().__init__(**kwargs)
        self.name = name
        self.query = query
        self.schema = schema
        self.presto_service_account_variable_key = presto_service_account_variable_key
        self.no_of_columns = no_of_columns
        self.no_of_rows = no_of_rows

    def execute(self, context):
        env = Variable.get("env", default_var="prod")
        presto_service_account_file = Variable.get(self.presto_service_account_variable_key)

        def get_presto_username():
            with open(presto_service_account_file, "r") as read_file:
                data = json.load(read_file)
                return data["username"]

        def get_presto_password():
            with open(presto_service_account_file, "r") as read_file:
                data = json.load(read_file)
                return data["password"]

        self.query = self.render_template(self.query, context)
        logging.info("Executing query :{}".format(self.query))
        conn = prestodb.dbapi.connect(
            host="presto-default.{}.twilio.com".format(env),
            port=8443,
            user=get_presto_username(),
            catalog='hive',
            schema=self.schema,
            http_scheme='https',
            auth=prestodb.auth.BasicAuthentication(
                get_presto_username(), get_presto_password())
        )

        cursor = conn.cursor()
        cursor.execute(self.query)
        results = cursor.fetchall()
        logging.info(results)
        if len(results) > 0:
            if self.no_of_columns >= 0 and self.no_of_columns != len(results[0]):
                raise Exception("No of columns not matching")
            if self.no_of_rows >= 0 and self.no_of_rows != len(results):
                raise Exception("No of rows not matching")
        else:
            logging.warning("No results returned for query: {} ".format(self.query))
        return results