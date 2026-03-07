

def sanity_checks(object_name,parameters,logger,session,rule_type,testtype,check_registry,Evaluation_registry):

    """
    Execute sanity checks 
    """
    sanity_results=[]
    status="ERROR"
    message="Unknown error during sanity check"

    try:
        logger.info(f"Running sanity checks for {object_name} with rule type {rule_type}")
        logger.info(f"object: {object_name}")
        logger.info(f"parameters: {parameters}")
        logger.info(f"rule type: {rule_type}")

        if rule_type not in check_registry:
            raise ValueError(f"Unsupported sanity check")
        check_function = check_registry[rule_type]

        logger.info(f"Running {rule_type} ")
        df=check_function(object_name,parameters,session)
        logger.info(f"Sanity check result: {df.show()}")
        value=df.count()
        logger.info(f"value:{value}")

        status="PASS" if value>0 else "FAIL"
        
    except Exception as e:
        status="ERROR"
        message=  str(e).split("\n")[-1].strip()

    if status=="PASS":
        logger.info(f"Sanity check passed: {rule_type} for {object_name}")
        message=f"Sanity check passed object {object_name} exists"
    elif status=="FAIL":
        logger.info(f"Sanity check failed: {rule_type} for {object_name}")
        message=f"Sanity check failed object {object_name} does not exist"
    else:
        logger.info(f"Sanity check error: {rule_type} for {object_name}")

    
    return  { 
                "rule_type":rule_type,
                "test_type":testtype,
                "object_name" :object_name,
                "status" : status,
                "message": message
            }




def functional_checks(object_name,parameters,logger,session,rule_type,testtype,check_registry,Evaluation_registry):
    """
    Execute functional checks for DQ validations with flexible evaluation
    
    Different rule types may return different results:
    """
    functional_results=[]
    status="ERROR"
    message="Unknown error during functional check"

    try:
        logger.info(f"Running Functional checks for {object_name} with rule type: {rule_type}")
        logger.info(f"object: {object_name}")
        logger.info(f"parameters: {parameters}")
        logger.info(f"rule type: {rule_type}")

        if rule_type not in check_registry:
            raise ValueError(f"Unsupported functional check {rule_type}")
        check_function = check_registry[rule_type]

        result=check_function(object_name,parameters,session)
        logger.info(f"Functional check result: {result}")

        # Evaluate the result based on rule type
        if rule_type not in Evaluation_registry:
            raise ValueError(f"Unsupported evaluation for rule type {rule_type}")
        evaluation_function = Evaluation_registry[rule_type]

        status, message  = evaluation_function(result, logger)
    
    except Exception as e:
        status="ERROR"
        message=  str(e).split("\n")[-1].strip()


    return  {
                "rule_type":rule_type,
                "test_type":testtype,
                "object_name" :object_name,
                "status" : status,
                "message": message
            }
