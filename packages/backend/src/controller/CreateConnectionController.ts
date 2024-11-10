// src/controllers/ScriptController.ts
import { Request, Response } from 'express';
import CreateConnectionService from '../service/CreateConnectionService';


class CreateConnectionController {
  public static async createConnection(req: Request, res: Response): Promise<void> {
    try {
      const { connName, dbType, host, port, user, password, database } = req.body;
      let schema = null;

      if (dbType == 'Snowflake') {
        const { requestSchema } = req.body;
        schema = requestSchema;
      }

      const result = await CreateConnectionService.createConnection(connName, dbType, host, port, user, password, database, schema);

      console.log("创建连接了=====> ", result)

      if (result.error != null) {
        const resultFinal = {
          'validationStatus': "ERROR",
          'information': result
        }

        res.status(200).json(resultFinal);
      } else {
        const resultFinal = {
          'validationStatus': "OK",
          'information': result
        }
        res.status(200).json(resultFinal);
      }

    } catch (error: any) {
      res.status(500).json({ error: 'Error running validation', details: error.message });
    }
  };

  public static async testConnection(req: Request, res: Response): Promise<void> {
    console.log("Connect successfully");
    res.status(200).json({ message: 'Connection test successful' });

  }
}


export default CreateConnectionController;