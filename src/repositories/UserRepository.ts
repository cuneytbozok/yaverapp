import { AppDataSource } from "../config/database";
import { User } from "../entities/User";
import { Repository } from "typeorm";

export class UserRepository {
  private repository: Repository<User>;

  constructor() {
    this.repository = AppDataSource.getRepository(User);
  }

  async create(userData: Partial<User>): Promise<User> {
    const user = this.repository.create(userData);
    return await this.repository.save(user);
  }

  async findById(id: string): Promise<User | null> {
    return await this.repository.findOne({ where: { id } });
  }

  async findByEmail(email: string): Promise<User | null> {
    return await this.repository.findOne({
      where: { email },
      select: ['id', 'email', 'username', 'password', 'createdAt', 'updatedAt']
    });
  }

  async findAll(): Promise<User[]> {
    return await this.repository.find();
  }
} 