import sys
import subprocess
import json

def create_connection(conn_name, db_type, host, port, user, password, database):
    connection_name = f"{conn_name}"

    # Build connection based on the database type
    if db_type == "MySQL":
        command = [
            "data-validation", "connections", "add",
            "--connection-name", connection_name, "MySQL",
            "--host", host,
            "--port", port,
            "--user", user,
            "--password", password,
            "--database", database
        ]
    elif db_type == "Postgres":
        command = [
            "data-validation", "connections", "add", 
            "--connection-name", connection_name, "Postgres",
            "--host", host,
            "--port", port,
            "--user", user,
            "--password", password,
            "--database", database
        ]
    elif db_type == "Oracle":
        command = [
            "data-validation", "connections", "add", 
            "--connection-name", connection_name, "Oracle",
            "--host", host,
            "--port", port,
            "--user", user,
            "--password", password,
            "--database", database
        ]
        
    elif db_type == "Snowflake": # Speical one
        command = [
            "data-validation", "connections", "add", 
            "--connection-name", connection_name, "Snowflake",
            "--user", user,
            "--password", password,
            "--account", host,
            "--database", database
        ]
        
    elif db_type == "MSSQL":
        command = [
            "data-validation", "connections", "add", 
            "--connection-name", connection_name, "MSSQL",
            "--host", host,
            "--port", port,
            "--user", user,
            "--password", password,
            "--database", database
        ]
        
    else:
        return {"error": f"Unsupported database type: {db_type}"}

    # catch all the errors
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        return {"connection_name": connection_name, "status": "success"}
    except subprocess.CalledProcessError as e:
        return {
            "error": "Connection command failed",
            "details": str(e),
            "stderr": e.stderr
        }
    
    except FileNotFoundError:
        return {"error": "data-validation tool not found"}
    
    except Exception as e:
        return {"error": "An unexpected error occurred", "details": str(e)}

def main():
    
    if len(sys.argv) != 8:
        print(json.dumps({"error": "Invalid input. Expected 6 arguments: db_type, host, port, user, password, database"}))
        return

    conn_name = sys.argv[1]
    db_type = sys.argv[2]
    host = sys.argv[3]
    port = sys.argv[4]
    user = sys.argv[5]
    password = sys.argv[6]
    database = sys.argv[7]

    
    result = create_connection(conn_name, db_type, host, port, user, password, database)
    print(json.dumps(result, ensure_ascii=False))
    
    return json.dumps(result, ensure_ascii=False)

if __name__ == "__main__":
    main()