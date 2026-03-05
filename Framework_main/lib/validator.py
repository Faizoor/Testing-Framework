
def sanity_checks(object_name,parameters,logger,session,rule_type,check_registry):

    """
    Execute sanity checks 
    """

    sanity_results=[]

    logger.info(f"Running sanity checks")
    logger.info(f"object: {object_name}")
    logger.info(f"parameters: {parameters}")
    logger.info(f"rule type: {rule_type}")

    if rule_type not in check_registry:
        raise ValueError(f"Unsupported sanity check")
    check_function = check_registry[rule_type]

    logger.info(f"Running {rule_type} ")
    df=check_function(object_name,parameters,session)
    value=df.count()
    logger.info(f"value:{value}")

    status="PASS" if value>0 else "FAIL"
    if status=="PASS":
        logger.info(f"Sanity check: rule  on object: {object_name} with rule type: {rule_type} has PASSED")
    else:
        logger.info(f"Sanity check: rule_id - on object: {object_name} with rule type: {rule_type} has FAILED")


    sanity_results.append(
            {
                "object_name" :object_name,
                "rule_type":rule_type,
                "status" : status
            }
    )
    logger.info(f"sanity result: {sanity_results}")

    return sanity_results


def functional_checks(object_name,parameters,logger,session,rule_type,check_registry):
    '''
    Execute functional checks for DQ validations
    '''
    pass