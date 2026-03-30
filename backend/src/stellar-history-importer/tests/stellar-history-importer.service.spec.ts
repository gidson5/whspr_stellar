import { Test, TestingModule } from '@nestjs/testing';
import { getRepositoryToken } from '@nestjs/typeorm';
import { getQueueToken } from '@nestjs/bullmq';
import { ConfigService } from '@nestjs/config';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { NotFoundException } from '@nestjs/common';
import {
  StellarHistoryImporterService,
  StellarTxRecord,
} from '../stellar-history-importer.service';
import {
  HistoryImportJob,
  ImportJobStatus,
} from '../entities/history-import-job.entity';
import { HISTORY_IMPORT_QUEUE } from '../processors/history-import.processor';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeTx(hash: string, pagingToken?: string): StellarTxRecord {
  return {
    txHash: hash,
    ledger: 1000,
    createdAt: '2024-01-01T00:00:00Z',
    sourceAccount: 'GABCDE',
    feeCharged: '100',
    operationCount: 1,
    successful: true,
    envelopeXdr: 'envelope',
    resultXdr: 'result',
  };
}

function makeJob(overrides: Partial<HistoryImportJob> = {}): HistoryImportJob {
  return {
    id: 'job-uuid',
    walletId: 'wallet-uuid',
    status: ImportJobStatus.PENDING,
    totalImported: 0,
    cursor: undefined,
    errorMessage: undefined,
    startedAt: new Date('2024-01-01'),
    completedAt: undefined,
    ...overrides,
  } as HistoryImportJob;
}

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

const mockJobRepo = {
  create: jest.fn(),
  save: jest.fn(),
  findOne: jest.fn(),
  update: jest.fn(),
  manager: {
    query: jest.fn(),
  },
};

const mockQueue = {
  add: jest.fn(),
};

const mockEventEmitter = {
  emit: jest.fn(),
};

const mockConfigService = {
  get: jest.fn((key: string, defaultVal?: string) => {
    if (key === 'STELLAR_HORIZON_URL') return 'https://horizon-testnet.stellar.org';
    return defaultVal;
  }),
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('StellarHistoryImporterService', () => {
  let service: StellarHistoryImporterService;

  beforeEach(async () => {
    jest.clearAllMocks();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        StellarHistoryImporterService,
        { provide: getRepositoryToken(HistoryImportJob), useValue: mockJobRepo },
        { provide: getQueueToken(HISTORY_IMPORT_QUEUE), useValue: mockQueue },
        { provide: EventEmitter2, useValue: mockEventEmitter },
        { provide: ConfigService, useValue: mockConfigService },
      ],
    }).compile();

    service = module.get<StellarHistoryImporterService>(StellarHistoryImporterService);

    // Silence logger output during tests
    jest.spyOn(service['logger'], 'log').mockImplementation(() => undefined);
    jest.spyOn(service['logger'], 'error').mockImplementation(() => undefined);
  });

  // -------------------------------------------------------------------------
  // triggerImport
  // -------------------------------------------------------------------------

  describe('triggerImport', () => {
    it('should create a PENDING job and enqueue it', async () => {
      const job = makeJob();
      mockJobRepo.create.mockReturnValue(job);
      mockJobRepo.save.mockResolvedValue(job);
      mockJobRepo.update.mockResolvedValue(undefined);
      mockQueue.add.mockResolvedValue(undefined);

      const result = await service.triggerImport('wallet-uuid', 'GABC123');

      expect(mockJobRepo.update).toHaveBeenCalledWith(
        { walletId: 'wallet-uuid', status: ImportJobStatus.RUNNING },
        expect.objectContaining({ status: ImportJobStatus.FAILED }),
      );
      expect(mockJobRepo.create).toHaveBeenCalledWith(
        expect.objectContaining({ walletId: 'wallet-uuid', status: ImportJobStatus.PENDING }),
      );
      expect(mockJobRepo.save).toHaveBeenCalledWith(job);
      expect(mockQueue.add).toHaveBeenCalledWith(
        'import',
        { jobId: job.id, walletId: 'wallet-uuid', walletAddress: 'GABC123' },
        expect.any(Object),
      );
      expect(result).toBe(job);
    });
  });

  // -------------------------------------------------------------------------
  // importHistory
  // -------------------------------------------------------------------------

  describe('importHistory', () => {
    it('should mark job RUNNING then COMPLETED when no transactions found', async () => {
      mockJobRepo.findOne.mockResolvedValue(makeJob());
      mockJobRepo.update.mockResolvedValue(undefined);
      mockJobRepo.manager.query.mockResolvedValue([]);
      jest.spyOn(service, 'fetchTransactionPage').mockResolvedValue([]);

      await service.importHistory('job-uuid', 'wallet-uuid', 'GABC123');

      expect(mockJobRepo.update).toHaveBeenCalledWith('job-uuid', {
        status: ImportJobStatus.RUNNING,
      });
      expect(mockJobRepo.update).toHaveBeenCalledWith(
        'job-uuid',
        expect.objectContaining({ status: ImportJobStatus.COMPLETED }),
      );
      expect(mockEventEmitter.emit).toHaveBeenCalledWith(
        'wallet.history.imported',
        expect.objectContaining({ walletId: 'wallet-uuid' }),
      );
    });

    it('should import all pages and update progress after each', async () => {
      const page1 = [makeTx('hash1'), makeTx('hash2')];
      const page2: StellarTxRecord[] = [];

      mockJobRepo.findOne.mockResolvedValue(makeJob());
      mockJobRepo.update.mockResolvedValue(undefined);
      mockJobRepo.manager.query.mockResolvedValue([]); // no existing txs

      jest
        .spyOn(service, 'fetchTransactionPage')
        .mockResolvedValueOnce(page1)
        .mockResolvedValueOnce(page2);

      jest
        .spyOn(service, 'deduplicateByTxHash')
        .mockResolvedValue(page1);

      jest
        .spyOn(service as any, 'persistTransactions')
        .mockResolvedValue(undefined);

      await service.importHistory('job-uuid', 'wallet-uuid', 'GABC123');

      expect((service as any).persistTransactions).toHaveBeenCalledWith(
        'wallet-uuid',
        page1,
      );
      expect(mockJobRepo.update).toHaveBeenCalledWith(
        'job-uuid',
        expect.objectContaining({ totalImported: 2 }),
      );
      expect(mockJobRepo.update).toHaveBeenCalledWith(
        'job-uuid',
        expect.objectContaining({ status: ImportJobStatus.COMPLETED, totalImported: 2 }),
      );
    });

    it('should mark job FAILED when an error occurs', async () => {
      mockJobRepo.findOne.mockResolvedValue(makeJob());
      mockJobRepo.update.mockResolvedValue(undefined);
      jest
        .spyOn(service, 'fetchTransactionPage')
        .mockRejectedValue(new Error('horizon down'));

      await expect(
        service.importHistory('job-uuid', 'wallet-uuid', 'GABC123'),
      ).rejects.toThrow('horizon down');

      expect(mockJobRepo.update).toHaveBeenCalledWith(
        'job-uuid',
        expect.objectContaining({ status: ImportJobStatus.FAILED }),
      );
    });

    it('should throw NotFoundException when job does not exist', async () => {
      mockJobRepo.findOne.mockResolvedValue(null);

      await expect(
        service.importHistory('bad-id', 'wallet-uuid', 'GABC123'),
      ).rejects.toThrow(NotFoundException);
    });

    it('should resume from saved cursor on retry', async () => {
      const jobWithCursor = makeJob({ cursor: 'cursor-abc', totalImported: 5 });
      mockJobRepo.findOne.mockResolvedValue(jobWithCursor);
      mockJobRepo.update.mockResolvedValue(undefined);

      const spy = jest.spyOn(service, 'fetchTransactionPage').mockResolvedValue([]);

      await service.importHistory('job-uuid', 'wallet-uuid', 'GABC123');

      expect(spy).toHaveBeenCalledWith('GABC123', 'cursor-abc');
    });
  });

  // -------------------------------------------------------------------------
  // mapHorizonTxToInternal
  // -------------------------------------------------------------------------

  describe('mapHorizonTxToInternal', () => {
    it('should correctly map all fields from a Horizon record', () => {
      const raw = {
        hash: 'abc123',
        ledger: 999,
        created_at: '2024-06-01T12:00:00Z',
        source_account: 'GABCDE',
        fee_charged: '200',
        operation_count: 2,
        successful: true,
        memo: 'hello',
        envelope_xdr: 'xdr1',
        result_xdr: 'xdr2',
        paging_token: 'token123',
      };

      const result = service.mapHorizonTxToInternal(raw);

      expect(result).toEqual({
        txHash: 'abc123',
        ledger: 999,
        createdAt: '2024-06-01T12:00:00Z',
        sourceAccount: 'GABCDE',
        feeCharged: '200',
        operationCount: 2,
        successful: true,
        memo: 'hello',
        envelopeXdr: 'xdr1',
        resultXdr: 'xdr2',
      });
    });

    it('should handle missing memo gracefully', () => {
      const raw = {
        hash: 'abc',
        ledger: 1,
        created_at: '2024-01-01T00:00:00Z',
        source_account: 'G',
        fee_charged: '100',
        operation_count: 1,
        successful: true,
        envelope_xdr: '',
        result_xdr: '',
      };

      const result = service.mapHorizonTxToInternal(raw);
      expect(result.memo).toBeUndefined();
    });
  });

  // -------------------------------------------------------------------------
  // deduplicateByTxHash
  // -------------------------------------------------------------------------

  describe('deduplicateByTxHash', () => {
    it('should filter out already-imported transactions', async () => {
      mockJobRepo.manager.query.mockResolvedValue([{ txHash: 'hash1' }]);
      const records = [makeTx('hash1'), makeTx('hash2')];

      const result = await service.deduplicateByTxHash('wallet-uuid', records);

      expect(result).toHaveLength(1);
      expect(result[0].txHash).toBe('hash2');
    });

    it('should return all records when none exist in DB', async () => {
      mockJobRepo.manager.query.mockResolvedValue([]);
      const records = [makeTx('hash1'), makeTx('hash2')];

      const result = await service.deduplicateByTxHash('wallet-uuid', records);

      expect(result).toHaveLength(2);
    });

    it('should return all records when the table does not exist', async () => {
      mockJobRepo.manager.query.mockRejectedValue(
        new Error('relation "stellar_transactions" does not exist'),
      );
      const records = [makeTx('hash1')];

      const result = await service.deduplicateByTxHash('wallet-uuid', records);

      expect(result).toHaveLength(1);
    });

    it('should return empty array when given empty input', async () => {
      const result = await service.deduplicateByTxHash('wallet-uuid', []);
      expect(result).toEqual([]);
    });
  });

  // -------------------------------------------------------------------------
  // getImportStatus
  // -------------------------------------------------------------------------

  describe('getImportStatus', () => {
    it('should return status DTO for the most recent job', async () => {
      const job = makeJob({ status: ImportJobStatus.COMPLETED, totalImported: 42 });
      mockJobRepo.findOne.mockResolvedValue(job);

      const result = await service.getImportStatus('wallet-uuid');

      expect(result.jobId).toBe(job.id);
      expect(result.status).toBe(ImportJobStatus.COMPLETED);
      expect(result.totalImported).toBe(42);
    });

    it('should throw NotFoundException when no job exists for wallet', async () => {
      mockJobRepo.findOne.mockResolvedValue(null);

      await expect(service.getImportStatus('wallet-uuid')).rejects.toThrow(
        NotFoundException,
      );
    });
  });

  // -------------------------------------------------------------------------
  // syncNewTransactions
  // -------------------------------------------------------------------------

  describe('syncNewTransactions', () => {
    it('should sync only new transactions since last cursor', async () => {
      const completedJob = makeJob({ status: ImportJobStatus.COMPLETED, cursor: 'cursor-xyz' });
      mockJobRepo.findOne.mockResolvedValue(completedJob);

      const newTxs = [makeTx('hashNew')];
      jest.spyOn(service, 'fetchTransactionPage').mockResolvedValue(newTxs);
      jest.spyOn(service, 'deduplicateByTxHash').mockResolvedValue(newTxs);
      jest.spyOn(service as any, 'persistTransactions').mockResolvedValue(undefined);

      const count = await service.syncNewTransactions('wallet-uuid', 'GABC123');

      expect(service.fetchTransactionPage).toHaveBeenCalledWith('GABC123', 'cursor-xyz');
      expect(count).toBe(1);
    });

    it('should return 0 when no new transactions found', async () => {
      mockJobRepo.findOne.mockResolvedValue(null); // no completed job
      jest.spyOn(service, 'fetchTransactionPage').mockResolvedValue([]);
      jest.spyOn(service, 'deduplicateByTxHash').mockResolvedValue([]);

      const count = await service.syncNewTransactions('wallet-uuid', 'GABC123');

      expect(count).toBe(0);
    });
  });

  // -------------------------------------------------------------------------
  // resumeFromCursor
  // -------------------------------------------------------------------------

  describe('resumeFromCursor', () => {
    it('should call importHistory with the stored walletId', async () => {
      const job = makeJob({ cursor: 'cursor-abc' });
      mockJobRepo.findOne.mockResolvedValue(job);
      const spy = jest
        .spyOn(service, 'importHistory')
        .mockResolvedValue(undefined);

      await service.resumeFromCursor('job-uuid', 'GABC123');

      expect(spy).toHaveBeenCalledWith('job-uuid', 'wallet-uuid', 'GABC123');
    });

    it('should throw NotFoundException for unknown job', async () => {
      mockJobRepo.findOne.mockResolvedValue(null);

      await expect(
        service.resumeFromCursor('bad-id', 'GABC123'),
      ).rejects.toThrow(NotFoundException);
    });
  });
});
