import subprocess
import json
import csv
import sys
import os
import mysql.connector

# get all tables' name
def get_table_names(database_config):
    try:
        
        connection = mysql.connector.connect(**database_config)
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        cursor.close()
        connection.close()

        if (len(tables) == 0 ):
            # error
            return 0

        # print(f"Validating table: {tables}")
        return tables
    except mysql.connector.Error as error:
        raise ConnectionError(f"Database connection error: {error}")

def validate_tables(tables, source_conn, target_conn, schema):        
    
    if (len(tables) == 0): 
        print(f"No tables in the target databse")
        return
    
    final_result = []

    # Create the target path 
    # save_path = os.path.join(os.path.dirname(__file__), '../../src/validation_script/column_validation_results.csv')

    # os.makedirs(os.path.dirname(save_path), exist_ok=True)

    # Only generate csv file when 'resultType' is 'CSV'

    # with open(save_path, mode="w", newline="") as file:
    #     writer = csv.writer(file)
    #     writer.writerow([
    #         "Validation Name", "Validation Type", "Source Table Name", "Source Column Name",
    #         "Source Aggregation Value", "Target Aggregation Value", "Percentage Difference",
    #         "Validation Status", "Run ID"
    #     ])

    for table_name in tables:

        # Command for column validation
        command = [
            "data-validation", "validate", "column",
            "-sc", source_conn,
            "-tc", target_conn,
            # SOURCE_SCHEMA.SOURCE_TABLE
            "-tbls", f"{schema}.{table_name}",
            "--count", "*",
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
                #         record.get("target_agg_value", ""),
                #         record.get("pct_difference", ""),
                #         record.get("validation_status", ""),
                #         record.get("run_id", "")
                #     ])
                
        except subprocess.CalledProcessError as e:
            # TODO: 处理 还没有创建连接 的报错
            print(f"Table {table_name} error while processing: {e}")
            print(e.output)
                
    return final_result

    
def main():
    
    # Validate the input vales -  sys.argv[0] is the python script file name
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
    tagret_conn = sys.argv[6]
    schema = sys.argv[7]
    
    try:
        tables = get_table_names(db_config)
        
        results = validate_tables(tables, source_conn, tagret_conn, schema)
        
        return json.dumps({"results": results}, ensure_ascii=False)
        
    except Exception as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)
        

if __name__ == "__main__":
    
    result = main()
    print(result)