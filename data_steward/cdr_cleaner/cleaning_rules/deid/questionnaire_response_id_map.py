"""
Maps questionnaire_response_ids from the observation table to the research_response_id in the
_deid_questionnaire_response_map lookup table.

Original Issue: DC-1347, DC-518, DC-2065

The purpose of this cleaning rule is to use the questionnaire mapping lookup table to remap the questionnaire_response_id 
in the observation table to the randomly generated research_response_id in the _deid_questionnaire_response_map table.
"""

# Python imports
import logging

# Project imports
from utils import pipeline_logging
from common import OBSERVATION, DEID_QUESTIONNAIRE_RESPONSE_MAP, JINJA_ENV
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC1347', 'DC518', 'DC-2065']

# Map the research_response_id from _deid_questionnaire_response_map lookup table to the questionnaire_response_id in
# the observation table
QRID_RID_MAPPING_QUERY = JINJA_ENV.from_string("""
UPDATE `{{project_id}}.{{dataset_id}}.observation` t
SET t.questionnaire_response_id = d.research_response_id
FROM (
    SELECT
        o.* EXCEPT (questionnaire_response_id),
        m.research_response_id
    FROM `{{project_id}}.{{dataset_id}}.observation` o
    LEFT JOIN `{{project_id}}.{{deid_questionnaire_response_map_dataset_id}}.{{deid_questionnaire_response_map}}` m
    ON o.questionnaire_response_id = m.questionnaire_response_id
    ) d
WHERE t.observation_id = d.observation_id
""")


class QRIDtoRID(BaseCleaningRule):
    """
    Remap the QID (questionnaire_response_id) from the
    observation table to the RID (research_response_id) found in that deid questionnaire response mapping lookup table
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None,
                 deid_questionnaire_response_map_dataset=None):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = f'Remap the QID (questionnaire_response_id) from the observation table to the ' \
               f'RID (research_response_id) found in ' \
               f'the deid questionnaire response mapping lookup table.'

        if not deid_questionnaire_response_map_dataset:
            raise TypeError(
                "`deid_questionnaire_response_map_dataset` cannot be empty")

        self.deid_questionnaire_response_map_dataset = deid_questionnaire_response_map_dataset

        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[
                             cdr_consts.CONTROLLED_TIER_DEID,
                             cdr_consts.REGISTERED_TIER_DEID
                         ],
                         affected_tables=OBSERVATION,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

    def get_query_specs(self, *args, **keyword_args):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries.  Each dictionary contains a
            single query and a specification for how to execute that query.
            The specifications are optional but the query is required.
        """

        mapping_query = {
            cdr_consts.QUERY:
                QRID_RID_MAPPING_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    deid_questionnaire_response_map=
                    DEID_QUESTIONNAIRE_RESPONSE_MAP,
                    deid_questionnaire_response_map_dataset_id=self.
                    deid_questionnaire_response_map_dataset)
        }

        return [mapping_query]

    def setup_rule(self, client, *args, **keyword_args):
        """
        Function to run any data upload options before executing a query.
        """
        pass

    def get_sandbox_tablenames(self):
        return [self.sandbox_table_for(table) for table in self.affected_tables]

    def setup_validation(self, client, *args, **keyword_args):
        """
        Run required steps for validation setup
        """
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client, *args, **keyword_args):
        """
        Validates the cleaning rule which deletes or updates the data from the tables
        """
        raise NotImplementedError("Please fix me.")


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ext_parser = parser.get_argument_parser()
    ext_parser.add_argument(
        '-q',
        '--deid_questionnaire_response_map_dataset',
        action='store',
        dest='deid_questionnaire_response_map_dataset',
        help=
        'Identifies the dataset containing the _deid_questionnaire_response_map lookup table',
        required=True)
    ARGS = ext_parser.parse_args()

    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id, [(QRIDtoRID,)],
            deid_questionnaire_response_map_dataset=ARGS.
            deid_questionnaire_response_map_dataset)
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id,
                                   ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id, [(QRIDtoRID,)],
                                   deid_questionnaire_response_map_dataset=ARGS.
                                   deid_questionnaire_response_map_dataset)
