from core import functional_evaluator, validator
from lib import checks


TEST_REGISTRY = {
    "sanity": validator.sanity_checks,
    "functional": validator.functional_checks
}

check_registry= {
    #sanity checks
    "stage_exists" : checks.stage_exists,
    "table_exists" : checks.table_exists,

    #functional checks
    "null_check" : checks.null_check,
    "duplicate_check" : checks.duplicate_check,
    "count_check" : checks.count_check

}

Evaluation_registry={
    "null_check": functional_evaluator._evaluate_null_check,
    "duplicate_check": functional_evaluator._evaluate_duplicate_check,
    "count_check": functional_evaluator._evaluate_count_check
}