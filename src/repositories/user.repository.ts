import { AppDataSource } from "../config/database";
import { User } from "../entities/User";

export class UserRepository {
  private repository = AppDataSource.getRepository(User);

  async findById(id: string): Promise<User | null> {
    return this.repository.findOne({ where: { id } });
  }

  async findByEmail(email: string): Promise<User | null> {
    return this.repository.findOne({
      where: { email },
      select: ['id', 'email', 'username', 'password', 'createdAt', 'updatedAt']
    });
  }

  async create(userData: Partial<User>): Promise<User> {
    const user = this.repository.create(userData);
    return await this.repository.save(user);
  }
}

export const userRepository = new UserRepository(); 