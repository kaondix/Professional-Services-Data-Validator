import subprocess
import json
import csv
import sys
import os
import mysql.connector


# Get all tables
def get_table_names(database_config):
    try:
        connection = mysql.connector.connect(**database_config)
        cursor = connection.cursor()
        
        cursor.execute("""
            SELECT kcu.TABLE_NAME, kcu.COLUMN_NAME AS primary_key_column, c.COLUMN_NAME
            FROM information_schema.TABLE_CONSTRAINTS tc
            JOIN information_schema.KEY_COLUMN_USAGE kcu
            ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            AND tc.TABLE_NAME = kcu.TABLE_NAME
            JOIN information_schema.COLUMNS c
            ON c.TABLE_NAME = kcu.TABLE_NAME
            AND c.TABLE_SCHEMA = tc.TABLE_SCHEMA
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            AND kcu.TABLE_SCHEMA = %s;
        """, (database_config['database'],))
        
        database_info = cursor.fetchall()
        
        # Store the info of each database
        tables_info = {}
        
        for table_name, primary_key_column, column_name in database_info:
            if table_name not in tables_info:
                tables_info[table_name] = {'primary_key': primary_key_column, 'columns': []}
            tables_info[table_name]['columns'].append(column_name)
        
        cursor.close()
        connection.close()

        if not tables_info:
            print("No tables with primary key found in the target databasr")
            return []
        
        return tables_info
    
    except mysql.connector.Error as error:
        raise ConnectionError(f"Database connection error: {error}")

# Row validation
def validate_rows(table_info, source_conn, target_conn, schema):
    
    if not table_info: 
        print("No tables with primary keys in the target database.")
        return

    final_result = []
    
    # Create the target path
    # save_path = os.path.join(os.path.dirname(__file__), '../../src/validation_script/row_validation_results.csv')

    # os.makedirs(os.path.dirname(save_path), exist_ok=True)

    # with open(save_path, mode="w", newline="") as file:
    #     writer = csv.writer(file)
    #     writer.writerow([
    #         "Validation Name", "Validation Type", "Source Table Name", "Source Column Name",
    #         "Source Aggregation Value", "Target Table Name", "Target Column Name",
    #         "Target Aggregation Value", "Group By Columns", "Primary Keys", "Number of Random Rows",
    #         "Difference", "Percentage Difference", "Percentage Threshold", "Validation Status",
    #         "Run ID", "Start Time", "End Time"
    #     ])

    for table_name, info in table_info.items():
        primary_key = info['primary_key']  # Use the primary key
        columns = info['columns'] # Compare each column in all database

        columns = info['columns']
        comparison_fields = ','.join(columns)
        # print(f"Comapring: {table_name}'s {comparison_fields}")
                    
        # Command for row validation
        command = [
            "data-validation", "validate", "row",
            "-sc", source_conn,
            "-tc", target_conn,
            # SOURCE_SCHEMA.SOURCE_TABLE
            "-tbls", f"{schema}.{table_name}",
            "--primary-keys", primary_key,
            "--comparison-fields", comparison_fields,
            "--format", "json"
        ]

        try:
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            result_data = json.loads(result.stdout)
            final_result.append(result_data)

                # for key, record in result_data.items():
                #     writer.writerow([
                #         record.get("validation_name", ""),
                #         record.get("validation_type", ""),
                #         record.get("source_table_name", ""),
                #         record.get("source_column_name", ""),
                #         record.get("source_agg_value", ""),
                #         record.get("target_table_name", ""),
                #         record.get("target_column_name", ""),
                #         record.get("target_agg_value", ""),
                #         record.get("group_by_columns", ""),
                #         record.get("primary_keys", ""),
                #         record.get("num_random_rows", ""),
                #         record.get("difference", ""),
                #         record.get("pct_difference", ""),
                #         record.get("pct_threshold", ""),
                #         record.get("validation_status", ""),
                #         record.get("run_id", ""),
                #         record.get("start_time", ""),
                #         record.get("end_time", "")
                #     ])

        except subprocess.CalledProcessError as e:
            print(f"Error processing table {table_name}: {e}")
            print(e.output)
                
    return final_result

def main():
    # Validate input values
    if len(sys.argv) != 8:
        print("Input is not valid")
        return
    
    db_config = {
        'host': sys.argv[1],
        'user': sys.argv[2],
        'password': sys.argv[3],
        'database': sys.argv[4]
    }
        
    source_conn = sys.argv[5]
    target_conn = sys.argv[6]
    schema = sys.argv[7]

    try:
        tables = get_table_names(db_config)
        row_results = validate_rows(tables, source_conn, target_conn, schema)
        return json.dumps({"results": row_results}, ensure_ascii=False)
    
    except Exception as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

if __name__ == "__main__":
    result = main()
    print(result)