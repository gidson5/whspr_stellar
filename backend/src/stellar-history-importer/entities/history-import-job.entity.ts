import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  Index,
} from 'typeorm';

export enum ImportJobStatus {
  PENDING = 'PENDING',
  RUNNING = 'RUNNING',
  COMPLETED = 'COMPLETED',
  FAILED = 'FAILED',
}

@Entity('history_import_jobs')
@Index(['walletId'])
@Index(['status'])
export class HistoryImportJob {
  @PrimaryGeneratedColumn('uuid')
  id!: string;

  @Column('uuid')
  walletId!: string;

  @Column({
    type: 'enum',
    enum: ImportJobStatus,
    default: ImportJobStatus.PENDING,
  })
  status!: ImportJobStatus;

  @Column({ type: 'int', default: 0 })
  totalImported!: number;

  /** Horizon paging_token of the last successfully imported transaction */
  @Column({ length: 100, nullable: true })
  cursor?: string;

  @Column({ type: 'text', nullable: true })
  errorMessage?: string;

  @CreateDateColumn()
  startedAt!: Date;

  @Column({ type: 'timestamp', nullable: true })
  completedAt?: Date;
}
