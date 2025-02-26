import { Request, Response, NextFunction } from 'express';
import { validateEnv } from '../config/env.validation'; 

validateEnv();

export const errorHandler = (err: Error, req: Request, res: Response, next: NextFunction) => {
  console.error(err.stack);
  res.status(500).json({ error: 'An unexpected error occurred' });
}; 