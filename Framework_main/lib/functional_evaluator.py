def _evaluate_null_check(result, logger):
    if not isinstance(result, int):
        status = "ERROR"
        message = f"Invalid result type for null check: {type(result)}. Expected an integer value"
        return status, message

    if result == 0:
        status = "PASS"
        message = "Null check passed: No null values found."
    else:
        status = "FAIL"
        message = f"Null check failed: Found {result} null values."

    return status, message
    

def _evaluate_duplicate_check():
    pass

def _evaluate_count_check(result, logger):

    if not isinstance(result, dict):
        status = "ERROR"
        message = f"Invalid result type for count check: {type(result)}. Expected a dictionary with source_count and target_count"
        return status, message

    source_count = result.get("source_count")
    target_count = result.get("target_count")

    if source_count is None or target_count is None:
        status = "ERROR"
        message = "Count is missigng for either source_count or target_count"
        return status, message

    if source_count == target_count:
        status = "PASS"
        message = f"Count check passed: source count {source_count} matches target count {target_count}"
    else:
        status = "FAIL"
        message = f"Count check failed: source count {source_count} does not match target count {target_count}" 

    return status, message 