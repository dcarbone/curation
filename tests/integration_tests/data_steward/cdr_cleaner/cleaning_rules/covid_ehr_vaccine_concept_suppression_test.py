"""
Integration test for covid_ehr_vaccine_concept_suppression module

None

Original Issue: DC1692
"""

# Python Imports
import os

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.covid_ehr_vaccine_concept_suppression import CovidEHRVaccineConceptSuppression
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import JINJA_ENV, OBSERVATION, CONCEPT, CONCEPT_RELATIONSHIP, CONCEPT_ANCESTOR

# Third party imports
from dateutil.parser import parse


class CovidEHRVaccineConceptSuppressionTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # set the test project identifier
        project_id = os.environ.get(PROJECT_ID)
        cls.project_id = project_id

        # set the expected test datasets
        dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id
        cls.vocabulary_id = os.environ.get('VOCABULARY_DATASET')

        # Update cutoff_date if necessary. "2022-01-01" is for May 2022 CDR.
        cutoff_date = "2022-01-01"
        cls.kwargs.update({'cutoff_date': cutoff_date})

        cls.rule_instance = CovidEHRVaccineConceptSuppression(
            project_id, dataset_id, sandbox_id)

        cls.vocab_tables = [CONCEPT, CONCEPT_RELATIONSHIP, CONCEPT_ANCESTOR]

        cls.fq_table_names = [
            f'{project_id}.{dataset_id}.{CONCEPT}',
            f'{project_id}.{dataset_id}.{CONCEPT_RELATIONSHIP}',
            f'{project_id}.{dataset_id}.{CONCEPT_ANCESTOR}',
            f'{project_id}.{dataset_id}.{OBSERVATION}',
        ]

        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(OBSERVATION)}'
        )

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        self.date = parse('2020-05-05').date()

        # Copy all needed vocab tables to the dataset
        for table in self.vocab_tables:
            self.client.copy_table(
                f'{self.project_id}.{self.vocabulary_id}.{table}',
                f'{self.project_id}.{self.dataset_id}.{table}')

        super().setUp()

    def test_covid_ehr_vaccine_concept_suppression_cleaning(self):
        """
        Tests that the specifications perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.        
        """

        INSERT_OBSERVATIONS_QUERY = JINJA_ENV.from_string("""
            INSERT INTO `{{fq_dataset_name}}.observation`
                (observation_id, person_id, observation_concept_id, observation_source_concept_id, observation_date, 
                observation_type_concept_id)
            VALUES
                -- Concepts via name, vocab, and code --
                -- Not suppressed: valid_start_date < cutoff_date - 1 year --                
                (1, 101, 724904, 0, date('2020-05-05'), 1),
                -- Suppressed: valid_start_date >= cutoff_date - 1 year --
                (2, 102, 0, 759434, date('2020-05-05'), 2),
                (3, 103, 42796198, 0, date('2020-05-05'), 3),

                -- Concepts via relationship --
                -- Not suppressed: valid_start_date < cutoff_date - 1 year --                
                (4, 116, 766241, 0, date('2020-05-05'), 2),
                (5, 109, 3548105, 0, date('2020-05-05'), 3),
                -- Suppressed: valid_start_date >= cutoff_date - 1 year --
                (6, 110, 0, 702673, date('2020-05-05'), 3),

                -- Concepts via ancestor --
                -- Suppressed: valid_start_date >= cutoff_date - 1 year --
                (7, 104, 37003432, 0, date('2020-05-05'), 3),
                (8, 104, 0, 37301121, date('2020-05-05'), 3),

                -- Not suppressed --
                (9, 115, 55, 0, date('2020-05-05'), 2),
                (10, 116, 0, 98, date('2020-05-05'), 1),
                
                -- Additional test for suppression (DC-2140) -- 
                -- Suppressed: valid_start_date >= cutoff_date - 1 year --
                (11, 117, 702866, 0, date('2020-05-05'), 1),
                (12, 118, 0, 822172, date('2020-05-05'), 2),
                (13, 119, 1219271, 0, date('2020-05-05'), 3),
                (14, 120, 0, 37003431, date('2020-05-05'), 1),
                (15, 121, 42794278, 0, date('2020-05-05'), 2),
                (16, 122, 0, 829421, date('2020-05-05'), 3),
                (17, 123, 37003431, 0, date('2020-05-05'), 1),
                (18, 124, 0, 947817, date('2020-05-05'), 2)
        """).render(fq_dataset_name=self.fq_dataset_name)

        queries = [INSERT_OBSERVATIONS_QUERY]

        self.load_test_data(queries)

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, OBSERVATION]),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18
            ],
            'sandboxed_ids': [2, 3, 6, 7, 8, 11, 12, 13, 14, 15, 16, 17, 18],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_source_concept_id', 'observation_date',
                'observation_type_concept_id'
            ],
            'cleaned_values': [(1, 101, 724904, 0, self.date, 1),
                               (4, 116, 766241, 0, self.date, 2),
                               (5, 109, 3548105, 0, self.date, 3),
                               (9, 115, 55, 0, self.date, 2),
                               (10, 116, 0, 98, self.date, 1)]
        }]

        self.default_test(tables_and_counts)
