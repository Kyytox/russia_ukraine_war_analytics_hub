import duckdb
from prefect import task

# Variables
from core.config.paths import PATH_DB_QUALIF_DATALAKE


@task(
    name="Insert data into DuckDB",
    task_run_name="insert-data-into-duckdb",
)
def insert_data_into_duckdb(df, query):
    """
    Insert data into DuckDB

    Args:
        df: dataframe with data
        query: query to insert data

    Returns:
        Number of inserted rows
    """

    con = duckdb.connect(database=PATH_DB_QUALIF_DATALAKE, read_only=False)

    try:
        insert = con.execute(query).fetchall()
        con.commit()
        return len(insert)

    except Exception as e:
        print(f"Error insert: {e}")
        con.rollback()
        return 0

    finally:
        con.close()


@task(
    name="Get data into DuckDB",
    task_run_name="get-data-into-duckdb",
)
def get_data_into_duckdb(query):
    """
    Get data into DuckDB

    Args:
        query: query to insert data

    Returns:
        Dataframe with data
    """

    con = duckdb.connect(database=PATH_DB_QUALIF_DATALAKE, read_only=False)

    # get data into DuckDB (no duplicates)
    df = con.execute(query).fetchdf()

    con.close()

    return df


@task(
    name="Update messages theme in DuckDB",
    task_run_name="update-messages-theme-duckdb",
)
def update_messages_theme_in_duckdb(df_results, table_name="main.messages_theme"):
    """
    Update messages_theme table with classification results.

    Args:
        df_results: dataframe with id_message, theme_inc_railway, terms_found_inc_railway
        table_name: table name to update (default: main.messages_theme)

    Returns:
        Number of updated rows
    """

    con = duckdb.connect(database=PATH_DB_QUALIF_DATALAKE, read_only=False)

    try:
        # Register dataframe as temporary table
        con.register("temp_results", df_results)

        # Update messages_theme with results
        con.execute(f"""
            UPDATE {table_name}
            SET 
                theme_inc_railway = temp_results.theme_inc_railway,
                terms_found_inc_railway = temp_results.terms_found_inc_railway
            FROM temp_results
            WHERE {table_name}.id_message = temp_results.id_message
        """)

        con.commit()

        print(f"{df_results.shape[0]} rows updated in {table_name}.")
        return

    except Exception as e:
        print(f"Error updating {table_name}: {e}")
        con.rollback()
        return 0

    finally:
        con.close()


@task(
    name="Update Table DuckDB",
    task_run_name="update-table-duckdb",
)
def update_table_duckdb(df, query):
    """
    Update DuckDB table

    Args:
        df: dataframe
        query: query to update data
    """

    con = duckdb.connect(database=PATH_DB_QUALIF_DATALAKE, read_only=False)

    try:
        con.execute(query)
        con.commit()
        print(f"{df.shape[0]} rows updated in DuckDB.")
        return

    except Exception as e:
        print(f"Error updating {table_name}: {e}")
        con.rollback()
        return 0

    finally:
        con.close()
