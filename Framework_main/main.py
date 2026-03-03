################################################################################################
# Snowpark Data Quality Testing Framework
################################################################################################
import yaml 
from snowflake.snowpark import Session
from dotenv import load_dotenv
import os 
import logging 
from lib import checks, validator, config_load
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
        #logger.info(self.config.get('tables'))
        logger.info(self.config)
        
    


    def execute(self):
        """ 
            Main function that will control the orchestration 
        """

        ##Start tests
        run_type = self.config['execution_config']
        logger.info(f"Test type we are going to check now: {run_type['run_type']}")




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
    yaml_path = "Framework_main/config/config.yaml"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )
    logger=logging.getLogger(("snowpark_testing"))

    logging.getLogger("snowflake").setLevel(logging.WARNING)
    logging.getLogger("snowflake.snowpark").setLevel(logging.WARNING)

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
        snowpark_testing.write_results_csv(results)


    except Exception as e :
        logger.info(f"Script aborted due to this error {type(e)}:  {e}")




