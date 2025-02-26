import { Entity, PrimaryGeneratedColumn, Column, CreateDateColumn, ManyToOne, JoinColumn } from "typeorm";
import { User } from "./User";

@Entity()
export class DataPoint {
  @PrimaryGeneratedColumn("uuid")
  id!: string;

  @Column()
  userId!: string;

  @Column("float")
  value!: number;

  @CreateDateColumn()
  timestamp!: Date;

  @Column()
  category!: string;

  @Column("jsonb", { nullable: true })
  metadata?: Record<string, any>;

  @ManyToOne(() => User, user => user.dataPoints)
  @JoinColumn({ name: "userId" })
  user!: User;
} 