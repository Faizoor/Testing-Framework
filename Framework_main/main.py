################################################################################################
# Snowpark Data Quality Testing Framework
################################################################################################
from snowflake.snowpark import Session
from dotenv import load_dotenv
from pathlib import Path
import os 
import logging 
from core import registry
from utils import config_load
import csv
from datetime import datetime


class SnowparkValidationRunner():
    """ 
        Main class for running Sanity and Functional checks for Data Quality Testing Framework.
            - sanity_checks: checks to validate the existence of objects and basic checks 
            - functional_checks: checks to validate the data quality based on the rules defined in the config file
            parameters:
                session: Snowflake session object
                yaml_path: path to the yaml config file 
    """

    def __init__(self,session,yaml_path):
        """
            Initialize the class with session and yaml path, load the config.
        """

        self.session= session
        self.yaml_path = yaml_path
        self.config = config_load.config_loader(yaml_path)

         


    def execute(self):
        """ 
            Main function that will control the orchestration 
        """

        ##Start tests
        test_results=[]
        run_type = self.config['execution_config']['run_type']
        logger.info(f"Test type we are going to check now: {run_type}")

        logger.info(f"Processing test cases one by one")
        test_cases=self.config['testcases']

        try:
            for test_case in test_cases:

                #Run only the test cases which are enabled 
                if not test_case.get('enabled', False):
                    logger.info(f"Skipping disabled test case: {test_case['rule_type']} the for object {test_case['object_name']}")
                    continue
                object_name = test_case['object_name']
                parameters = test_case['parameters']
                testtype = test_case['test_type'].strip().lower()
                rule_type= test_case['rule_type'].strip().lower()


                test_function = registry.TEST_REGISTRY.get(testtype)


                if not test_function:
                    logger.info(f"unknown function Identified please register this function {test_function} in the test registry")
                    continue

                test_result=test_function(
                    object_name,
                    parameters,
                    logger,
                    self.session,
                    rule_type,
                    testtype,
                    registry.check_registry,
                    registry.Evaluation_registry
                )
                #logger.info(f"result: {test_result}")
                test_results.append(test_result)

            #logger.info(f"test resutls: {test_results}")
        except Exception as e :
            logger.error(f"Need to add the failure message:{type(e)} {str(e)}")

        return test_results


    def _get_summary(self,results):
        """
        Return summary of the checks with total, passed, failed and error counts
        """
        total_checks = len(results)
        passed_checks = len([r for r in results if r['status'] == 'PASS'])
        failed_checks = len([r for r in results if r['status'] == 'FAIL'])
        error_checks = len([r for r in results if r['status'] == 'ERROR'])
        return {
            "total": total_checks,
            "passed": passed_checks,
            "failed": failed_checks,
            "error": error_checks
        }
        


    def write_results_csv(self,results):
        """
        log the summary and write the results to a csv file in the local directory or a specified location
        """
        file_path = f"reports/test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        summary=self._get_summary(results)

        logger.info("===== Data Quality Report =====")
        logger.info(f"Total Tests : {summary['total']}")
        logger.info(f"Passed      : {summary['passed']}")
        logger.info(f"Failed      : {summary['failed']}")
        logger.info(f"Errors      : {summary['error']}")

        with open(file_path, mode='w', newline='') as file:
            writer=csv.DictWriter(file,fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)
        logger.info(f"Results written to {file_path}")
       

        
        

if __name__ == "__main__":
    """
    Main function to execute the testing framework
    """

    load_dotenv()
    base_dir = Path(__file__).resolve().parent
    yaml_path = base_dir/"config"/"config.yaml"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )
    logger=logging.getLogger(("snowpark_testing"))

    logging.getLogger("snowflake").setLevel(logging.ERROR)
    logging.getLogger("snowflake.snowpark").setLevel(logging.ERROR)

    connection_parameters = {
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "user": os.environ["SNOWFLAKE_USER"],
    "password": os.environ["SNOWFLAKE_PASSWORD"],
    "role": os.environ["SNOWFLAKE_ROLE"], 
}

    #create session
    try:
        session = Session.builder.configs(connection_parameters).create()
    
    #create Framework Object 
        snowpark_testing = SnowparkValidationRunner( session, yaml_path  )
        results=snowpark_testing.execute()
        logger.info(f"results: {results}")

    # Export the output as csv format 
        snowpark_testing.write_results_csv(results)


    except Exception as e :
        logger.info(f"Script aborted due to this error {type(e)}:  {e}")




