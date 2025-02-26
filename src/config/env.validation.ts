import { cleanEnv, str, port } from 'envalid';

export const validateEnv = () => {
  return cleanEnv(process.env, {
    NODE_ENV: str({ choices: ['development', 'test', 'production'] }),
    PORT: port({ default: 3000 }),
    JWT_SECRET: str(),
    DB_HOST: str({ default: 'localhost' }),
    DB_PORT: port({ default: 5432 }),
    DB_USERNAME: str(),
    DB_PASSWORD: str(),
    DB_NAME: str(),
  });
}; 