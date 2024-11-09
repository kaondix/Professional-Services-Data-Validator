// src/controllers/ScriptController.ts
import { Request, Response } from 'express';
import ValidationService from '../service/ValidationService';


class ValidationController {
    public static async validationColumn(req: Request, res: Response): Promise<void> {

        console.log("Validating column...")

        try {
            const { host, user, password, database, source_conn, target_conn } = req.body;
            const result = await ValidationService.validationColumn(host, user, password, database, source_conn, target_conn);
            const jsonResult = JSON.parse(result);
            // console.log("===> ", jsonResult)

            if (jsonResult.error != null) {
                const resultFinal = {
                    'validationStatus': "ERROR",
                    'information': jsonResult
                }

                res.status(200).json(resultFinal);
            } else {
                const resultFinal = {
                    'validationStatus': "OK",
                    'information': jsonResult
                }
                res.status(200).json(resultFinal);
            }

        } catch (error: any) {
            res.status(500).json({ error: 'Error running validation', details: error.message });
        }
    };

    public static async validationRow(req: Request, res: Response): Promise<void> {

        console.log("Validating row...")

        try {
            const { host, user, password, database, source_conn, target_conn } = req.body;
            const result = await ValidationService.validationRow(host, user, password, database, source_conn, target_conn);
            const jsonResult = JSON.parse(result);
            // console.log("===> ", jsonResult)

            if (jsonResult.error != null) {
                const resultFinal = {
                    'validationStatus': "ERROR",
                    'information': jsonResult
                }

                res.status(200).json(resultFinal);
            } else {
                const resultFinal = {
                    'validationStatus': "OK",
                    'information': jsonResult
                }
                res.status(200).json(resultFinal);
            }
        } catch (error: any) {
            res.status(500).json({ error: 'Error running validation', details: error.message });
        }

    }
}


export default ValidationController;