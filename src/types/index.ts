// Define core types for the application
export interface User {
  id: string;
  username: string;
  email: string;
  createdAt: Date;
  updatedAt: Date;
}

export interface DataPoint {
  id: string;
  userId: string;
  value: number;
  timestamp: Date;
  category: string;
  metadata?: Record<string, any>;
} 