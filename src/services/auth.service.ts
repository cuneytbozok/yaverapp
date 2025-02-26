import bcrypt from 'bcrypt';
import jwt from 'jsonwebtoken';
import { UserRepository } from '../repositories/UserRepository';
import { User } from '../entities/User';

export class AuthService {
  private userRepository: UserRepository;

  constructor() {
    this.userRepository = new UserRepository();
  }

  async register(userData: { username: string; email: string; password: string }): Promise<{ user: Omit<User, 'password'>; token: string }> {
    const hashedPassword = await bcrypt.hash(userData.password, 10);
    const user = await this.userRepository.create({
      ...userData,
      password: hashedPassword
    });

    const { password, ...userWithoutPassword } = user;
    const token = this.generateToken(user.id);
    return { user: userWithoutPassword, token };
  }

  async login(email: string, password: string): Promise<{ user: Omit<User, 'password'>; token: string }> {
    const user = await this.userRepository.findByEmail(email);
    if (!user) {
      throw new Error('Invalid login credentials');
    }

    const isPasswordValid = await bcrypt.compare(password, user.password);
    if (!isPasswordValid) {
      throw new Error('Invalid login credentials');
    }

    const { password: _, ...userWithoutPassword } = user;
    const token = this.generateToken(user.id);
    return { user: userWithoutPassword, token };
  }

  private generateToken(userId: string): string {
    return jwt.sign({ id: userId }, process.env.JWT_SECRET || 'your-secret-key', {
      expiresIn: '24h'
    });
  }
} 