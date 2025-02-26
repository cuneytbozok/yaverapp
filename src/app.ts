import express from 'express';
import { config } from 'dotenv';
import { AppDataSource } from './config/database';
import "reflect-metadata";
import userRoutes from './routes/user.routes';
import authRoutes from './routes/auth.routes';
import { errorHandler } from './middleware/error.middleware';
import rateLimit from 'express-rate-limit';
import helmet from 'helmet';

// Initialize environment variables
config();

const app = express();
const port = process.env.PORT || 3000;

// Security middleware
app.use(helmet());

// Rate limiting middleware
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100, // Limit each IP to 100 requests per windowMs
  message: 'Too many requests from this IP, please try again later.'
});
app.use(limiter);

// Middleware
app.use(express.json());

// Routes
app.use('/api/auth', authRoutes);
app.use('/api/users', userRoutes);

// Error handling middleware
app.use(errorHandler);

// Initialize Database
async function initializeDatabase() {
  try {
    await AppDataSource.initialize();
    console.log("Database connection established");
  } catch (error) {
    console.error("Error connecting to database:", error);
    process.exit(1);
  }
}

initializeDatabase();

// Add graceful shutdown
process.on('SIGTERM', async () => {
  console.log('Received SIGTERM. Performing graceful shutdown...');
  await AppDataSource.destroy();
  process.exit(0);
});

// Basic health check route
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'healthy' });
});

// Export the app for testing
export default app;

// Start the server only if not in test mode
if (process.env.NODE_ENV !== 'test') {
  app.listen(port, () => {
    console.log(`Server running on port ${port}`);
  });
} 