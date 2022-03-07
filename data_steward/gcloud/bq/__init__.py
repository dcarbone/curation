"""
Interact with Google Cloud BigQuery
"""
# Python stl imports
import os
import typing

# Third-party imports
from google.cloud import bigquery
from google.cloud.bigquery import Client
from google.auth import default
from opentelemetry import trace
from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# Project imports
from utils import auth
from resources import fields_for
from constants.utils import bq as consts
from common import JINJA_ENV


class BigQueryClient(Client):
    """
    A client that extends GCBQ functionality
    See https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.client.Client.html#google.cloud.bigquery.client.Client
    """

    def __init__(self, project_id: str, scopes=None, credentials=None):
        """
        :param project_id: Identifies the project to create a cloud BigQuery client for
        :param scopes: List of Google scopes as strings
        :param credentials: Google credentials object (ignored if scopes is defined,
            uses delegated credentials instead)

        :return:  A BigQueryClient instance
        """
        tracer_provider = TracerProvider()
        cloud_trace_exporter = CloudTraceSpanExporter(project_id=project_id)
        tracer_provider.add_span_processor(
            BatchSpanProcessor(cloud_trace_exporter))
        trace.set_tracer_provider(tracer_provider)
        tracer = trace.get_tracer(__name__)
        with tracer.start_as_current_span(project_id):
            if scopes:
                credentials, project_id = default()
                credentials = auth.delegated_credentials(credentials,
                                                         scopes=scopes)
            super().__init__(project=project_id, credentials=credentials)

    def get_table_schema(self, table_name: str, fields=None) -> list:
        """
        A helper method to create big query SchemaFields for dictionary definitions.

        Given the table name, reads the schema from the schema definition file
        and returns a list of SchemaField objects that can be used for table
        creation.

        :param table_name:  the table name to get BigQuery SchemaField information
            for.
        :param fields: An optional argument to provide fields/schema as a list of JSON objects
        :returns:  a list of SchemaField objects representing the table's schema.
        """
        if fields:
            fields = fields
        else:
            fields = fields_for(table_name)

        schema = []
        for column in fields:
            name = column.get('name')
            field_type = column.get('type')
            column_def = bigquery.SchemaField(name,
                                              field_type).from_api_repr(column)

            schema.append(column_def)

        return schema

    def _to_standard_sql_type(self, field_type: str) -> str:
        """
        Get standard SQL type corresponding to a SchemaField type

        :param field_type: type in SchemaField object (can be legacy or standard SQL type)
        :return: standard SQL type name
        """
        upper_field_type = field_type.upper()
        standard_sql_type_code = bigquery.schema.LEGACY_TO_STANDARD_TYPES.get(
            upper_field_type)
        if not standard_sql_type_code:
            raise ValueError(f'{field_type} is not a valid field type')
        standard_sql_type = bigquery.StandardSqlDataTypes(
            standard_sql_type_code)
        return standard_sql_type.name

    def _to_sql_field(self,
                      field: bigquery.SchemaField) -> bigquery.SchemaField:
        """
        Convert all types in a schema field object to standard SQL types (not legacy)

        :param field: the schema field object
        :return: a converted schema field object
        """
        return bigquery.SchemaField(
            field.name, self._to_standard_sql_type(field.field_type),
            field.mode, field.description, field.fields)

    def get_create_or_replace_table_ddl(
            self,
            dataset_id: str,
            table_id: str,
            schema: typing.List[bigquery.SchemaField] = None,
            cluster_by_cols: typing.List[str] = None,
            as_query: str = None,
            **table_options) -> str:
        """
        Generate CREATE OR REPLACE TABLE DDL statement

        Note: Reference https://bit.ly/3fgkCPg for supported syntax

        :param dataset_id: identifies the dataset containing the table
        :param table_id: identifies the table to be created or replaced
        :param schema: list of schema fields (optional). if not provided, attempts to
                    use a schema associated with the table_id.
        :param cluster_by_cols: columns defining the table clustering (optional)
        :param as_query: query used to populate the table (optional)
        :param table_options: options e.g. description and labels (optional)
        :return: DDL statement as string
        """
        CREATE_OR_REPLACE_TABLE_TPL = JINJA_ENV.from_string(
            consts.CREATE_OR_REPLACE_TABLE_QUERY)
        _schema = self.get_table_schema(table_id) if schema is None else schema
        _schema = [self._to_sql_field(field) for field in _schema]
        return CREATE_OR_REPLACE_TABLE_TPL.render(
            project_id=self.project,
            dataset_id=dataset_id,
            table_id=table_id,
            schema=_schema,
            cluster_by_cols=cluster_by_cols,
            query=as_query,
            opts=table_options)

    def dataset_columns_query(self, dataset_id: str) -> str:
        """
        Get INFORMATION_SCHEMA.COLUMNS query for a specified dataset

        :param dataset_id: identifies the dataset whose metadata is queried
        :return the query as a string
        """
        DATASET_COLUMNS_TPL = JINJA_ENV.from_string(
            consts.DATASET_COLUMNS_QUERY)
        return DATASET_COLUMNS_TPL.render(project_id=self.project,
                                          dataset_id=dataset_id)

    def define_dataset(self, dataset_id: str, description: str,
                       label_or_tag: dict) -> bigquery.Dataset:
        """
        Define the dataset reference.

        :param dataset_id:  string name of the dataset id to return a reference of
        :param description:  description for the dataset
        :param label_or_tag:  labels for the dataset = Dict[str, str]
                            tags for the dataset = Dict[str, '']

        :return: a dataset reference object.
        :raises: google.api_core.exceptions.Conflict if the dataset already exists
        """
        if not description or description.isspace():
            raise RuntimeError("Provide a description to create a dataset.")

        if not dataset_id:
            raise RuntimeError("Provide a dataset_id")

        if not label_or_tag:
            raise RuntimeError("Please provide a label or tag")

        dataset_id = f"{self.project}.{dataset_id}"

        # Construct a full Dataset object to send to the API.
        dataset = bigquery.Dataset(dataset_id)
        dataset.description = description
        dataset.labels = label_or_tag
        dataset.location = "US"

        return dataset