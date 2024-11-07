import { spawn } from 'child_process';
import path from 'path';

class CreateConnectionService {
    public static async createConnection(connName: string, dbType: string, host: string, port: string, user: string, password: string, database: string): Promise<any> {
        return new Promise((resolve, reject) => {

            console.log("===Go into CreateConnectionService===")

            const scriptPath = path.join(__dirname, '../../validation_script/connection.py');

            const process = spawn('python', [scriptPath, connName, dbType, host, port, user, password, database]);

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
                        reject(new Error('Failed to parse Python script output as JSON'));
                    }
                } else {
                    reject(new Error(`Python script error: ${errorOutput}`));
                }
            });
        });
    }
}

export default CreateConnectionService;