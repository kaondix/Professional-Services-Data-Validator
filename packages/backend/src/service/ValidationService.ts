import { spawn } from 'child_process';
import path from 'path';

class ValidationService {

    public static async validationColumn(host: string, user: string, password: string, database: string, source_conn: string, target_conn: string): Promise<any> {
        return new Promise((resolve, reject) => {

            const scriptPath = path.join(__dirname, '../../src/validation_script/column_validation.py');

            const process = spawn('python', [scriptPath, host, user, password, database, source_conn, target_conn]);

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
                        console.log(output)

                        // Python script's output was already in json format
                        resolve(output);
                    } catch (parseError) {
                        reject(new Error('Failed to parse Python script output as JSON'));
                    }
                } else {
                    reject(new Error(`Python script error: ${errorOutput}`));
                }
            });
        });
    };


    public static async validationRow(host: string, user: string, password: string, database: string, source_conn: string, target_conn: string): Promise<any> {
        return new Promise((resolve, reject) => {

            const scriptPath = path.join(__dirname, '../../src/validation_script/row_validation.py');

            const process = spawn('python', [scriptPath, host, user, password, database, source_conn, target_conn]);

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
                        console.log(output)

                        // Python script's output was already in json format
                        resolve(output);
                    } catch (parseError) {
                        reject(new Error('Failed to parse Python script output as JSON'));
                    }
                } else {
                    reject(new Error(`Python script error: ${errorOutput}`));
                }
            });
        });

    };

    // Method for JSON column validation result
    public static validateJSONColumn(targetResult: JSON) {

    };

    // Method for JSON row validation result
    public static validateJSONRow(targetResult: JSON) {

    };


}

export default ValidationService