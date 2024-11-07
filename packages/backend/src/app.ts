// src/app.ts
import express from 'express';
import CreateRouter from './routers/CreateRoute';
import ValidationRouter from './routers/ValidationRoute';

const app = express();

app.use(express.json());

app.use('/api', CreateRouter);
app.use('/api/validation', ValidationRouter);

export default app;