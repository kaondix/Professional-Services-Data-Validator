
import { Router } from 'express';
import ValidationController from '../controller/ValidationController';

const ValidationRouter = Router();

ValidationRouter.post('/column', ValidationController.validationColumn);
ValidationRouter.get('/row', ValidationController.validationRow);

export default ValidationRouter;