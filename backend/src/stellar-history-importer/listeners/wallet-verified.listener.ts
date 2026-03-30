import { Injectable, Logger } from '@nestjs/common';
import { OnEvent } from '@nestjs/event-emitter';
import { StellarHistoryImporterService } from '../stellar-history-importer.service';

export interface WalletVerifiedEvent {
  walletId: string;
  walletAddress: string;
}

/**
 * Listens for wallet.verified events (emitted by WalletIntegrationService after
 * a successful wallet connection) and automatically queues a history import.
 */
@Injectable()
export class WalletVerifiedListener {
  private readonly logger = new Logger(WalletVerifiedListener.name);

  constructor(private readonly importerService: StellarHistoryImporterService) {}

  @OnEvent('wallet.verified')
  async handleWalletVerified(event: WalletVerifiedEvent): Promise<void> {
    this.logger.log(
      `wallet.verified event received — queuing history import for wallet ${event.walletId}`,
    );
    await this.importerService.triggerImport(event.walletId, event.walletAddress);
  }
}
