import request from 'supertest';
import app from '../app';
import { AppDataSource } from '../config/database';
import { User } from '../entities/User';
import bcrypt from 'bcrypt';
import { AuthService } from '../services/auth.service';
import jwt from 'jsonwebtoken';

// Mock AuthService
jest.mock('../services/auth.service', () => {
  return {
    AuthService: jest.fn().mockImplementation(() => ({
      register: jest.fn().mockImplementation(async (userData) => ({
        user: {
          id: '1',
          ...userData,
          createdAt: new Date(),
          updatedAt: new Date()
        },
        token: 'mock-token'
      })),
      login: jest.fn().mockImplementation(async (email, password) => {
        if (email === 'test@example.com' && password === 'password123') {
          return {
            user: {
              id: '1',
              email,
              username: 'testuser',
              createdAt: new Date(),
              updatedAt: new Date()
            },
            token: 'mock-token'
          };
        }
        throw new Error('Invalid login credentials');
      })
    }))
  };
});

// Mock the AppDataSource
jest.mock('../config/database', () => {
  const mockRepository = {
    findOne: jest.fn().mockImplementation(({ where: { id } }) => {
      if (id === '1') {
        return Promise.resolve({
          id: '1',
          username: 'testuser',
          email: 'test@example.com',
          password: 'hashedpassword',
          createdAt: new Date(),
          updatedAt: new Date()
        });
      }
      return null;
    }),
    create: jest.fn(),
    save: jest.fn().mockImplementation(data => ({
      id: '1',
      ...data,
      createdAt: new Date(),
      updatedAt: new Date()
    })),
  };

  return {
    AppDataSource: {
      initialize: jest.fn().mockResolvedValueOnce(true),
      getRepository: jest.fn().mockReturnValue(mockRepository),
    },
  };
});

// Add this mock before your tests
jest.mock('jsonwebtoken', () => ({
  verify: jest.fn().mockReturnValue({ id: '1' }),
  sign: jest.fn().mockReturnValue('mock-token')
}));

describe('GET /health', () => {
  it('should return a healthy status', async () => {
    const response = await request(app).get('/health');
    expect(response.status).toBe(200);
    expect(response.body).toEqual({ status: 'healthy' });
  });
});

describe('POST /api/auth/register', () => {
  it('should register a new user', async () => {
    const response = await request(app)
      .post('/api/auth/register')
      .send({
        username: 'testuser',
        email: 'test@example.com',
        password: 'password123'
      });

    expect(response.status).toBe(201);
    expect(response.body).toHaveProperty('user');
    expect(response.body).toHaveProperty('token');
  });
});

describe('POST /api/auth/token', () => {
  it('should return a token for valid credentials', async () => {
    const response = await request(app)
      .post('/api/auth/token')
      .send({
        email: 'test@example.com',
        password: 'password123',
      });

    expect(response.status).toBe(200);
    expect(response.body).toHaveProperty('user');
    expect(response.body).toHaveProperty('token');
  });

  it('should return 401 for invalid password', async () => {
    const response = await request(app)
      .post('/api/auth/token')
      .send({
        email: 'test@example.com',
        password: 'wrongpassword',
      });

    expect(response.status).toBe(401);
    expect(response.body).toHaveProperty('error');
  });

  it('should return 401 for non-existent user', async () => {
    const response = await request(app)
      .post('/api/auth/token')
      .send({
        email: 'nonexistent@example.com',
        password: 'password123',
      });

    expect(response.status).toBe(401);
    expect(response.body).toHaveProperty('error');
  });

  it('should return 400 for invalid email format', async () => {
    const response = await request(app)
      .post('/api/auth/token')
      .send({
        email: 'invalid-email',
        password: 'password123',
      });

    expect(response.status).toBe(400);
    expect(response.body).toHaveProperty('errors');
  });
});

describe('GET /api/auth/login', () => {
  it('should return user data with valid token', async () => {
    const response = await request(app)
      .get('/api/auth/login')
      .set('Authorization', 'Bearer mock-token');

    expect(response.status).toBe(200);
    expect(response.body).toHaveProperty('id');
    expect(response.body).toHaveProperty('email');
  });

  it('should return 401 without token', async () => {
    const response = await request(app)
      .get('/api/auth/login');

    expect(response.status).toBe(401);
    expect(response.body).toHaveProperty('error');
  });
}); 