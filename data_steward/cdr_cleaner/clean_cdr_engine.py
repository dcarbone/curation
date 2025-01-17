# Python imports
import inspect
import logging
from concurrent.futures import TimeoutError as TOError

# Third party imports
import google.cloud.bigquery as gbq
from google.cloud.exceptions import GoogleCloudError

# Project imports
from gcloud.bq import BigQueryClient
from utils.auth import get_impersonation_credentials
from utils.pipeline_logging import configure
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants import bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as cdr_consts
from constants.cdr_cleaner import clean_cdr_engine as ce_consts
from common import CDR_SCOPES

LOGGER = logging.getLogger(__name__)


def add_console_logging(add_handler=True):
    """
    Using pipeline_logging's configure for logging purposes.
    """
    configure(add_console_handler=add_handler)


def clean_dataset(project_id,
                  dataset_id,
                  sandbox_dataset_id,
                  rules,
                  table_namer='',
                  run_as=None,
                  **kwargs):
    """
    Run the assigned cleaning rules and return list of BQ job objects

    :param project_id: identifies the project
    :param dataset_id: identifies the dataset to clean
    :param sandbox_dataset_id: identifies the sandbox dataset to store backup rows
    :param rules: a list of cleaning rule objects/functions as tuples
    :param table_namer: source differentiator value expected to be the same for all rules run on the same dataset
    :param run_as: email address of the service account to impersonate
    :param kwargs: keyword arguments a cleaning rule may require
    :return all_jobs: List of BigQuery job objects
    """
    # Set up client
    impersonation_creds = None
    if run_as:
        # get credentials and create client
        impersonation_creds = get_impersonation_credentials(
            run_as, target_scopes=CDR_SCOPES)
    client = BigQueryClient(project_id=project_id,
                            credentials=impersonation_creds)

    all_jobs = []
    for rule_index, rule in enumerate(rules):
        clazz = rule[0]
        query_function, setup_function, rule_info = infer_rule(
            clazz, project_id, dataset_id, sandbox_dataset_id, table_namer,
            **kwargs)

        LOGGER.info(
            f"Applying cleaning rule {rule_info[cdr_consts.MODULE_NAME]} "
            f"{rule_index+1}/{len(rules)}")
        setup_function(client)
        query_list = query_function()
        jobs = run_queries(client, query_list, rule_info)
        LOGGER.info(
            f"For clean rule {rule_info[cdr_consts.MODULE_NAME]}, {len(jobs)} jobs "
            f"were run successfully for {len(query_list)} queries")
        all_jobs.extend(jobs)
    return all_jobs


def generate_job_config(project_id, query_dict):
    """
    Generates BigQuery job_configuration object

    :param project_id: Identifies the project
    :param query_dict: dictionary for the query
    :return: BQ job_configuration object
    """
    job_config = gbq.job.QueryJobConfig()
    if query_dict.get(cdr_consts.DESTINATION_TABLE) is None:
        return job_config

    destination_table = gbq.TableReference.from_string(
        f'{project_id}.{query_dict[cdr_consts.DESTINATION_DATASET]}.{query_dict[cdr_consts.DESTINATION_TABLE]}'
    )

    job_config.destination = destination_table
    job_config.use_legacy_sql = query_dict.get(cdr_consts.LEGACY_SQL, False)
    # allow_large_results can only be used if use_legacy_sql=True
    job_config.allow_large_results = job_config.use_legacy_sql
    job_config.write_disposition = query_dict.get(cdr_consts.DISPOSITION,
                                                  bq_consts.WRITE_EMPTY)
    return job_config


def run_queries(client, query_list, rule_info):
    """
    Runs queries from the list of query_dicts

    :param client: a BigQueryClient
    :param query_list: list of query_dicts generated by a cleaning rule
    :param rule_info: contains information about the query function
    :return: integers indicating the number of queries that succeeded and failed
    """
    query_count = len(query_list)
    jobs = []
    for query_no, query_dict in enumerate(query_list):
        try:
            LOGGER.info(
                ce_consts.QUERY_RUN_MESSAGE_TEMPLATE.render(
                    query_no=query_no, query_count=query_count, **rule_info))
            job_config = generate_job_config(client.project, query_dict)

            module_short_name = rule_info[cdr_consts.MODULE_NAME].split(
                '.')[-1][:10]
            query_job = client.query(query=query_dict.get(cdr_consts.QUERY),
                                     job_config=job_config,
                                     job_id_prefix=f'{module_short_name}_')
            jobs.append(query_job)
            LOGGER.info(f'Running {query_job.job_id}')
            # wait for job to complete
            query_job.result()
            if query_job.errors:
                raise RuntimeError(
                    ce_consts.FAILURE_MESSAGE_TEMPLATE.render(
                        client.project, query_job, **rule_info, **query_dict))
            LOGGER.info(
                ce_consts.SUCCESS_MESSAGE_TEMPLATE.render(
                    project_id=client.project,
                    query_job=query_job,
                    query_no=query_no,
                    query_count=query_count,
                    **rule_info))
        except (GoogleCloudError, TOError) as exp:
            LOGGER.exception(
                ce_consts.FAILURE_MESSAGE_TEMPLATE.render(
                    project_id=client.project,
                    **rule_info,
                    **query_dict,
                    exception=exp))
            raise exp
    return jobs


def get_rule_args(clazz):
    """
    Gets list of ("param_name", Parameter)
    :param clazz: 
    :return: 
    """
    params = inspect.signature(clazz).parameters
    return [
        dict(name=name, required=param.default is inspect.Parameter.empty)
        for name, param in params.items()
    ]


def get_custom_kwargs(clazz, **kwargs):
    """
    Filters kwargs based on the signature of the 'clazz'

    :param clazz: Clean class or clean function to check the signature of
    :param kwargs: optional arguments provided by the user
    :return: filtered dictionary of kwargs
    :raises ValueError: if a required param for 'clazz' is missing from kwargs
    """
    params = inspect.signature(clazz).parameters
    rule_params = {
        k: v
        for k, v in params.items()
        if k not in ce_consts.CLEAN_ENGINE_REQUIRED_PARAMS
    }
    # filter kwargs based on required params
    kwargs = {k: v for k, v in kwargs.items() if k in rule_params.keys()}
    missing = [
        k for k, v in rule_params.items()
        if k not in kwargs.keys() and v.default is inspect.Parameter.empty
    ]
    if missing:
        raise ValueError(f'Params {missing} '
                         f'not provided for cleaning rule {clazz.__name__}')
    return kwargs


def infer_rule(clazz, project_id, dataset_id, sandbox_dataset_id, table_namer,
               **kwargs):
    """
    Extract information about the cleaning rule

    :param clazz: Clean rule class or old style clean function
    :param project_id: identifies the project
    :param dataset_id: identifies the dataset to clean
    :param sandbox_dataset_id: identifies the sandbox dataset to store backup rows
    :param table_namer: source differentiator value expected to be the same for all rules run on the same dataset
    :param kwargs: keyword arguments a cleaning rule may require
    :return:
        query_function: function that generates query_list
        setup_function: function that sets up the tables for the rule
        rule_info: dictionary of information about the rule
            keys:
                query_function: function that generates query_list
                setup_function: function that sets up the tables for the rule
                function_name: name of the query function
                module_name: name of the module containing the function
                line_no: points to the source line where query_function is
    """
    kwargs = get_custom_kwargs(clazz, **kwargs)
    if inspect.isclass(clazz) and issubclass(clazz, BaseCleaningRule):
        try:
            instance = clazz(project_id, dataset_id, sandbox_dataset_id,
                             table_namer, **kwargs)
        except TypeError as e:
            LOGGER.warning(f"{clazz.__name__} does not accept the "
                           f"`table_namer` property yet.")
            instance = clazz(project_id, dataset_id, sandbox_dataset_id,
                             **kwargs)
        query_function = instance.get_query_specs
        setup_function = instance.setup_rule
        function_name = query_function.__name__
        module_name = inspect.getmodule(query_function).__name__
        line_no = inspect.getsourcelines(query_function)[1]
    else:
        function_name = clazz.__name__
        module_name = inspect.getmodule(clazz).__name__
        line_no = inspect.getsourcelines(clazz)[1]

        def query_function():
            """
            Imitates base class get_query_specs()
            :return: list of query dicts generated by rule
            """
            return clazz(project_id, dataset_id, sandbox_dataset_id, **kwargs)

        def setup_function(client):
            """
            Imitates base class setup_rule()
            """
            pass

    rule_info = {
        cdr_consts.QUERY_FUNCTION: query_function,
        cdr_consts.SETUP_FUNCTION: setup_function,
        cdr_consts.FUNCTION_NAME: function_name,
        cdr_consts.MODULE_NAME: module_name,
        cdr_consts.LINE_NO: line_no,
    }
    return query_function, setup_function, rule_info


def get_query_list(project_id,
                   dataset_id,
                   sandbox_dataset_id,
                   rules,
                   table_namer='',
                   **kwargs):
    """
    Generates list of all query_dicts that will be run on the dataset

    :param project_id: identifies the project
    :param dataset_id: identifies the dataset to clean
    :param sandbox_dataset_id: identifies the sandbox dataset to store backup rows
    :param table_namer: source differentiator value expected to be the same for all rules run on the same dataset
    :param rules: a list of cleaning rule objects/functions as tuples
    :param kwargs: keyword arguments a cleaning rule may require
    :return list of all query_dicts that will be run on the dataset
    """
    all_queries_list = []
    for rule in rules:
        clazz = rule[0]
        query_function, _, rule_info = infer_rule(clazz, project_id, dataset_id,
                                                  sandbox_dataset_id,
                                                  table_namer, **kwargs)
        query_list = query_function()
        all_queries_list.extend(query_list)
    return all_queries_list
