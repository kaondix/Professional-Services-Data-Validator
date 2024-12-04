// src/app.ts
import express from 'express';
import CreateRouter from './routers/CreateRoute';
import cors from 'cors';
import ValidationRouter from './routers/ValidationRoute';

const app = express();

app.use(express.json());
app.use(cors({
    // TODO:
    origin: "http://localhost:8000",
    methods: 'GET,POST,PUT,DELETE,OPTIONS',
    allowedHeaders: 'Content-Type,Authorization'
}))

app.use('/api', CreateRouter);
app.use('/api/validation', ValidationRouter);

export default app;