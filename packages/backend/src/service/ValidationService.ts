import { spawn } from 'child_process';
import path from 'path';
import { Parser } from '@json2csv/plainjs'
import fs from 'fs'
import { json } from 'stream/consumers';

class ValidationService {

    public static async validationColumn(host: string, user: string, password: string, database: string, source_conn: string, target_conn: string, resultType: string, schema: string, content: string): Promise<any> {
        return new Promise((resolve, reject) => {

            const scriptPath = path.join(__dirname, '../../src/validation_script/column_validation.py');

            const process = spawn('python', [scriptPath, host, user, password, database, source_conn, target_conn, schema]);

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
                        // console.log(JSON.parse(output));

                        if (output.length != 0) {
                            if (resultType == 'CSV') {

                                // conver json into csv
                                this.convertJSONtoCSV(JSON.parse(output).results, 'column', content);
                                resolve(output);

                            } else {
                                resolve(output);
                            }
                        }

                    } catch (parseError) {
                        reject(new Error('Failed to parse Python script output as JSON'));
                    }
                } else {
                    reject(new Error(`Python script error: ${errorOutput}`));
                }
            });
        });
    };


    public static async validationRow(host: string, user: string, password: string, database: string, source_conn: string, target_conn: string, resultType: string, schema: string, content: string): Promise<any> {
        return new Promise((resolve, reject) => {

            const scriptPath = path.join(__dirname, '../../src/validation_script/row_validation.py');

            const process = spawn('python', [scriptPath, host, user, password, database, source_conn, target_conn, schema]);

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

                        if (resultType == 'CSV') {

                            // conver json into csv - TODO: error occur when converting into csv
                            this.convertJSONtoCSV(JSON.parse(output).results, 'row', content);
                            resolve(output);

                        } else {
                            resolve(output);
                        }
                    } catch (parseError) {
                        reject(new Error('Failed to parse Python script output as JSON'));
                    }
                } else {
                    reject(new Error(`Python script error: ${errorOutput}`));
                }
            });
        });

    };

    // Methods to convert the json data into a csv file - column validation
    public static convertJSONtoCSV(jsonData: Object[], type: string, content: string): string | Error {

        // Extract each nested object from the original json
        const preparedData = jsonData.flatMap(item => {
            return Object.values(item);
        });

        let finalData: Object[] = [];

        if (content === 'ONLY_FAILED') {
            finalData = preparedData.filter(item => item.validation_status === 'fail');
            if (finalData != null) { // Has failure item
                const result = this.writeCSV(finalData, type)
                return result;
            } else { // Transformation succeeded, no errors
                const result = this.writeCSV(preparedData, type);
                return result;
            }
        } else { // all data
            const result = this.writeCSV(preparedData, type);
            return result;
        }


    };

    public static writeCSV(inputData: Object[], type: string): string | Error {

        try {
            const fields = [
                { value: 'validation_name', label: "Validation Name" },
                { value: 'validation_type', label: "Validation Type" },
                { value: 'source_table_name', label: "Source Table Name" },
                { value: 'source_column_name', label: "Source Column Name" },
                { value: 'source_agg_value', label: "Source Aggregation Value" },
                { value: 'target_table_name', label: "Target Table Name" },
                { value: 'target_column_name', label: "Target Column Name" },
                { value: 'target_agg_value', label: "Target Aggregation Value" },
                { value: 'group_by_columns', label: "Group By Columns" },
                { value: 'primary_keys', label: "Primary Keys" },
                { value: 'num_random_rows', label: "Number of Random Rows" },
                { value: 'difference', label: "Difference" },
                { value: 'pct_difference', label: "Percentage Difference" },
                { value: 'pct_threshold', label: "Percentage Threshold" },
                { value: 'validation_status', label: "Validation Status" },
                { value: 'run_id', label: "Run ID" },
                { value: 'start_time', label: "Start Time" },
                { value: 'end_time', label: "End Time" }
            ];

            const opts = { fields };
            const parser = new Parser(opts);
            const csv = parser.parse(inputData);

            const current = new Date();
            const hours = current.getHours().toString().padStart(2, '0');
            const minutes = current.getMinutes().toString().padStart(2, '0');
            const seconds = current.getSeconds().toString().padStart(2, '0');

            const fileName = `${hours}${minutes}${seconds}`;
            // TODO: Change to the correct path - dbt-demo/seeds
            const savePath = `./src/${fileName}_${type}.csv`;
            fs.writeFileSync(savePath, csv);

            return "OK";

        } catch (error) {
            console.log("ERROR: ===> ", error);
            // Return error
            return error instanceof Error ? error : new Error(String(error));
        }

    }


}

export default ValidationService