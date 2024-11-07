// src/controllers/ScriptController.ts
import { Request, Response } from 'express';
import CreateConnectionService from '../service/CreateConnectionService';


class CreateConnectionController {
  public static async createConnection(req: Request, res: Response): Promise<void> {
    try {
      const { connName, dbType, host, port, user, password, database } = req.body;
      const result = await CreateConnectionService.createConnection(connName, dbType, host, port, user, password, database);
      res.status(200).json(result);
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