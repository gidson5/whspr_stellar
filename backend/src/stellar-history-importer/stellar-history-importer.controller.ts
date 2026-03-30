import {
  Controller,
  Post,
  Get,
  Param,
  Body,
  HttpCode,
  HttpStatus,
} from '@nestjs/common';
import { StellarHistoryImporterService } from './stellar-history-importer.service';
import { ImportStatusDto, TriggerImportResponseDto } from './dto/import-status.dto';

class TriggerImportBodyDto {
  walletAddress!: string;
}

@Controller('wallets')
export class StellarHistoryImporterController {
  constructor(private readonly importerService: StellarHistoryImporterService) {}

  /**
   * POST /wallets/:id/import-history
   * Trigger a full history import for the given wallet.
   */
  @Post(':id/import-history')
  @HttpCode(HttpStatus.ACCEPTED)
  async triggerImport(
    @Param('id') walletId: string,
    @Body() body: TriggerImportBodyDto,
  ): Promise<TriggerImportResponseDto> {
    const job = await this.importerService.triggerImport(walletId, body.walletAddress);
    return {
      jobId: job.id,
      message: 'History import queued successfully',
    };
  }

  /**
   * GET /wallets/:id/import-status
   * Return the latest import job status for the given wallet.
   */
  @Get(':id/import-status')
  async getImportStatus(@Param('id') walletId: string): Promise<ImportStatusDto> {
    return this.importerService.getImportStatus(walletId);
  }
}
