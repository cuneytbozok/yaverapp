import { config } from 'dotenv';

// Load environment variables before running tests
config();

// Override with test-specific values
process.env.NODE_ENV = 'test';
process.env.PORT = '3001';
process.env.DB_HOST = 'localhost';
process.env.DB_PORT = '5432';
process.env.DB_USERNAME = 'yaver_test';
process.env.DB_PASSWORD = 'yaver_test';
process.env.DB_NAME = 'yaver_test_db';
process.env.JWT_SECRET = 'test-jwt-secret-key'; 