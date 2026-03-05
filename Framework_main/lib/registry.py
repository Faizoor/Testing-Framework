from lib.validator import *
from lib.checks import *


TEST_REGISTRY = {
    "sanity": sanity_checks,
    "functional": functional_checks
}

check_registry= {
    "stage_exists" : stage_exists,
    "table_exists" : table_exists,
    "null_check" : null_check,
    "duplicate_check" : duplicate_check

}