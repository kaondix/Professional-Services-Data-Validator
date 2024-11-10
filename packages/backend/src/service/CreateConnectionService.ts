import { spawn } from 'child_process';
import path from 'path';

class CreateConnectionService {
    public static async createConnection(connName: string, dbType: string, host: string, port: string, user: string, password: string, database: string, schema: string): Promise<any> {
        return new Promise((resolve, reject) => {

            try {
                console.log("===Go into CreateConnectionService===")

                const scriptPath = path.join(__dirname, '../../src/validation_script/connection.py');

                let command: string[];

                if (dbType == 'Snowflake') {
                    const snowDB = `${database}/${schema}`;

                    console.log("snowflake的db===>", snowDB);
                    
                    command = [scriptPath, connName, dbType, host, port, user, password, snowDB];
                } else {
                    command = [scriptPath, connName, dbType, host, port, user, password, database];
                }

                const process = spawn('python', command);

                let output = '';
                let errorOutput = '';

                process.stdout.on('data', (data) => {
                    output += data.toString();
                });

                process.stderr.on('data', (data) => {
                    errorOutput += data.toString();
                });

                process.on('close', (code) => {
                    if (code === 0) {
                        try {
                            const result = JSON.parse(output); // assuming the Python script returns JSON
                            resolve(result);
                        } catch (parseError) {
                            console.log("运行时出错=====>", parseError);
                            reject(new Error('Failed to parse Python script output as JSON'));
                        }
                    } else {
                        reject(new Error(`Python script error: ${errorOutput}`));
                    }
                });
            } catch (error: any) {
                console.log("创建连接时出错=====> ", error);
                reject(new Error(error));
            }


        });
    }
}

export default CreateConnectionService;