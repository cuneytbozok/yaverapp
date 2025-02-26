import { AppDataSource } from "../config/database";
import { DataPoint } from "../entities/DataPoint";
import { Repository, Between } from "typeorm";

export class DataPointRepository {
  private repository: Repository<DataPoint>;

  constructor() {
    this.repository = AppDataSource.getRepository(DataPoint);
  }

  async create(dataPointData: Partial<DataPoint>): Promise<DataPoint> {
    const dataPoint = this.repository.create(dataPointData);
    return await this.repository.save(dataPoint);
  }

  async findById(id: string): Promise<DataPoint | null> {
    return await this.repository.findOne({ 
      where: { id },
      relations: ['user']
    });
  }

  async findByUserId(userId: string): Promise<DataPoint[]> {
    return await this.repository.find({
      where: { userId },
      relations: ['user']
    });
  }

  async findByDateRange(startDate: Date, endDate: Date): Promise<DataPoint[]> {
    return await this.repository.find({
      where: {
        timestamp: Between(startDate, endDate)
      },
      relations: ['user']
    });
  }
} 