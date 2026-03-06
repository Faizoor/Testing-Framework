# This Module contains SQL generation for Testing Framework checks
#   Function takes table names and rules as parameter and returns a generated SQL
#   

from snowflake.snowpark.functions import col



def null_check(object_name,parameters,session):
    """
    Check for null values in specified columns
    
    Parameters:
        - columns: List of column names to check
        
    Returns:
        DataFrame with rows containing null values
    """
    try:
        table_name=object_name.strip()

        df = session.table(table_name)
        column_names = parameters['column_name']

        condition = None

        # Build condition to check for nulls in any specified column
        for c in column_names:
            expr = col(c).is_null()
            condition = expr if condition is None else condition | expr

        df= df.filter(condition)

        return df.count()

    except Exception as e:
        raise ValueError(str(e))



def table_exists(object_name,parameters,session):
    """check if a table exists using snowpark dataframe"""
    try:
        Full_table_name=object_name.strip()
        parts = Full_table_name.split('.')

        database, schema, table_name = [p.upper() for p in parts]

        df=session.table(f"{database}.INFORMATION_SCHEMA.TABLES")\
                .filter(
                    (col("TABLE_SCHEMA")==schema) & 
                    (col("TABLE_NAME")==table_name)
                    )
        return df
        
    except Exception as e:
        raise ValueError(str(e)) 
  

def count_check(object_name,parameters,session):
    """
    Compare row counts between source and target tables
    
    Parameters:
        - target_table: Name of the target table to compare against
        - threshold: Optional - acceptable difference (default 0)
        
    Returns:
        Dict with source_count, target_count, and difference
    """
    try:
        source_table=object_name.strip()
        target_table=parameters['target_table'].strip()

        source_df=session.table(source_table)
        target_df=session.table(target_table)

        source_count=source_df.count()
        target_count=target_df.count()

        return {
            "source_count": source_count,
            "target_count": target_count,
            "difference": abs(source_count - target_count)
        }

    except Exception as e:
        raise ValueError(str(e)) 

def duplicate_check():
    pass


def stage_exists(object_name):
    pass






def null_check_with_sql(object_name,column_name):
    try:
        Full_table_name=object_name.strip()
        parts = Full_table_name.split('.')

        database, schema, table_name = [p.upper() for p in parts]

        condition=" OR ".join([f"{col} IS NULL" for col in column_name])


        generated_sql=f"""
                    SELECT count(*) as result
                    FROM {database}.{schema}.{table_name}
                    WHERE {condition};
                    """
        return generated_sql.strip()
    except Exception as e:
        raise ValueError(f"Error building null_check: {str(e)}")
    
def table_exists_with_sql(object_name):   
    """Check if a table exists in Snowflake"""
    try:
        Full_table_name=object_name.strip()
        parts = Full_table_name.split('.')

        database, schema, table_name = [p.upper() for p in parts]

        generated_sql=f"""
                    SELECT count(*) as result
                    FROM {database}.INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA='{schema}'
                    AND TABLE_NAME='{table_name}';
                    """
        return generated_sql.strip()
    except Exception as e:
        raise ValueError(f"Error building table_exists check: {str(e)}")    