<?php

namespace App\Jobs\Le;

use App\Enums\ExportStatus;
use App\Models\Export;
use Illuminate\Bus\Batch;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Database\Query\Builder;
use Illuminate\Contracts\Auth\Authenticatable;
use Illuminate\Support\Facades\Bus;
use Illuminate\Support\Str;
use Illuminate\Support\Facades\Log;

abstract class LeExportProcessor implements ShouldQueue
{
    protected ?Authenticatable $user;
    protected int $perPage;
    protected string $name;
    protected string $disk;
    protected string $directory;

    protected string $queueName = 'default';

    abstract public function query(): Builder;

    public function handle(): void
    {
        $this->initialize();
        $exportName = $this->name;
        $exportDisk = $this->disk;
        $exportDirectory = $this->directory;

        $totalRow = $this->query()->count();
        $totalPage = ceil($totalRow / $this->perPage);
        $batchUuid = Str::uuid();

        Log::info(sprintf('[%s] [%s] Start exporting', self::class, $batchUuid), [
            'totalRow' => $totalRow,
            'totalPage' => $totalPage,
            'perPage' => $this->perPage,
            'exportName' => $exportName,
            'exportDisk' => $exportDisk,
            'exportDirectory' => $exportDirectory,
            'queueName' => $this->queueName,
        ]);

        $export = $this->initializeExport($totalRow);

        if ($totalRow <= 0) {
            $export->update([
                'status' => ExportStatus::COMPLETED->value,
                'completed_at' => now(),
            ]);
            Log::info(sprintf('[%s] [%s] No records to export', self::class, $batchUuid), $export);
            return;
        }

        /** @var App\Jobs\Le\LeExportToCsv[] */
        $jobs = [];
        for ($page = 1; $page <= $totalPage; $page++) {
            $jobs[] = new LeExportToCsv($this, $page, $this->perPage, $batchUuid);
        }

        Log::info(sprintf('[%s] [%s] Jobs setup', self::class, $batchUuid), [
            'Total Jobs' => count($jobs)
        ]);

        Bus::batch($jobs)
            ->progress(function (Batch $batch) use ($export, $batchUuid) {
                $export->update([
                    'status' => ExportStatus::IN_PROGRESS->value,
                ]);

                // Log::info(sprintf('[%s] [%s] Batch progress', self::class, $batchUuid), [
                //     'export' => $export,
                //     'batch' => $batch
                // ]);
            })
            ->then(function (Batch $batch) use ($export, $batchUuid, $exportName, $exportDisk, $exportDirectory) {
                Log::info(sprintf('[%s] [%s] Batch then', self::class, $batchUuid), [
                    'export' => $export,
                    'batch' => $batch,
                    'exportName' => $exportName,
                    'exportDisk' => $exportDisk,
                    'exportDirectory' => $exportDirectory
                ]);

                dispatch(new LeCollateExportsAndUploadToDisk($export, $batchUuid, $exportName, $exportDisk, $exportDirectory, $this->queueName));
            })
            ->allowFailures()
            ->name($exportName)
            ->onQueue($this->queueName)
            ->dispatch();
    }

    private function initialize(): void
    {
        if (empty($this->name)) $this->name = class_basename($this);
        if (empty($this->disk)) $this->disk = 'public'; // TODO: env
        if (empty($this->perPage)) $this->perPage = 2000;
        if (empty($this->directory)) $this->directory = '';
    }

    private function initializeExport(int $totalRow): Export
    {
        return Export::query()->create([
            'user_id' => $this->user?->id ?? null,
            'user_type' => !empty($this->user) ? get_class($this->user) : null,
            'status' => ExportStatus::PENDING->value,
            'processor' => self::class,
            'file_total_rows' => $totalRow,
            'started_at' => now(),
        ]);
    }

    public function setUser(Authenticatable $user): self
    {
        $this->user = $user;
        return $this;
    }

    public function setPerPage(int $perPage): self
    {
        $this->perPage = $perPage;
        return $this;
    }

    public function setName(string $name): self
    {
        $this->name = $name;
        return $this;
    }

    public function setDisk(string $disk): self
    {
        $this->disk = $disk;
        return $this;
    }

    public function setDirectory(string $directory): self
    {
        $this->directory = $directory;
        return $this;
    }
}
