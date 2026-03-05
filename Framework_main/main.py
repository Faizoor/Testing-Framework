################################################################################################
# Snowpark Data Quality Testing Framework
################################################################################################
from snowflake.snowpark import Session
from dotenv import load_dotenv
from pathlib import Path
import os 
import logging 
from lib import checks, validator, config_load, registry
import pandas as pd
import csv


class SnowparkValidationRunner():
    """ 
        Main class for running Sanity and Functional checks for Data Quality Testing Framework.
            - sanity_checks: checks to validate the existence of objects and basic checks before running DQ validations
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
                #logger.info(f"test_case: {test_case}")
                object_name = test_case['object_name']
                parameters = test_case['parameters']
                testtype = test_case['test_type'].strip().lower()
                rule_type= test_case['rule_type'].strip().lower()
                #logger.info(f"test registry{registry.TEST_REGISTRY}")

                test_function = registry.TEST_REGISTRY.get(testtype)
                #print(test_function)

                if not test_function:
                    logger.info(f"unknown function Identified please register this function {test_function} in the test registry")
                    continue

                test_result=test_function(
                    object_name,
                    parameters,
                    logger,
                    self.session,
                    rule_type,
                    registry.check_registry
                )
                logger.info(f"result: {test_result}")
                test_results.append(test_result)

            logger.info(f"test resutls: {test_results}")
        except Exception as e :
            logger.error(f"Need to add the failure message:{type(e)} {str(e)}")

#        return {
#            "sanity":sanity_check_result,
#            "Functional":functional_check_result,
           
#        }

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
        Write the results to a csv file in the local directory
        """
        rows=[]

        for category in ["sanity","Functional"]:
            rows.extend(
                [{'category':category,**item}for item in results[category]]
            )

        df=pd.DataFrame(rows)
        df.to_csv("test_result.csv",index=False)

        logger.info(f"Results written to test_result.csv")
       

        
        

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
        #logger.info(f"results: {results}")

    # Export the output as csv format 
        #snowpark_testing.write_results_csv(results)


    except Exception as e :
        logger.info(f"Script aborted due to this error {type(e)}:  {e}")




