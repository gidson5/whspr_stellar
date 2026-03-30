import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Logger } from '@nestjs/common';
import { Job } from 'bullmq';
import { StellarHistoryImporterService } from '../stellar-history-importer.service';

export const HISTORY_IMPORT_QUEUE = 'stellar-history-import';

export interface HistoryImportJobData {
  jobId: string;
  walletId: string;
  walletAddress: string;
}

@Processor(HISTORY_IMPORT_QUEUE)
export class HistoryImportProcessor extends WorkerHost {
  private readonly logger = new Logger(HistoryImportProcessor.name);

  constructor(private readonly importerService: StellarHistoryImporterService) {
    super();
  }

  async process(job: Job<HistoryImportJobData>): Promise<void> {
    const { jobId, walletId, walletAddress } = job.data;
    this.logger.log(`Processing history import job ${jobId} for wallet ${walletId}`);

    try {
      await this.importerService.importHistory(jobId, walletId, walletAddress);
    } catch (error) {
      this.logger.error(`History import job ${jobId} failed: ${error.message}`);
      throw error;
    }
  }
}
