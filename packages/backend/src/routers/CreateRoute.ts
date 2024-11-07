
import { Router } from 'express';
import CreateConnectionController from '../controller/CreateConnectionController';

const CreateRouter = Router();

CreateRouter.post('/create-connection', CreateConnectionController.createConnection);
CreateRouter.get('/test-connection', CreateConnectionController.testConnection);

export default CreateRouter;