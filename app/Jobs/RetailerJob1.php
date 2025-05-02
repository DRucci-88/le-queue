<?php

namespace App\Jobs;

use App\Enums\ExportStatus;
use App\Models\Export;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Contracts\Auth\Authenticatable;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Bus;
use Illuminate\Support\Str;
use Illuminate\Support\Facades\Log;
use Carbon\Carbon;
use Illuminate\Bus\Batch;
use Illuminate\Database\Query\Builder;
use Throwable;

class RetailerJob1 implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable;

    protected ?Authenticatable $user = null;

    /** 
     * Create a new job instance.
     */
    public function __construct() {}

    public function query(): Builder {
        $startDateOfVisit = Carbon::make('2025-01-01');
        $endDateOfVisit = Carbon::make('2025-10-10');

        $startDatePull = Carbon::make('2025-01-01');
        $endDatePull = Carbon::make('2025-10-10');

        return DB::query()
            ->selectRaw('
            retailers.id,
            retailers.account_number,
            retailers.visit_frequency,
            retailers.hero_brand,
            retailers.fighter_sku,
            retailers.streak,
            retailers.date_of_visit,
            retailers.base_target_met,
            retailers.analog_incentive
        ')
            ->from('retailers')
            ->whereBetween('retailers.date_of_visit', [$startDateOfVisit, $endDateOfVisit])
            ->whereBetween('retailers.date_pull', [$startDatePull, $endDatePull])
            ->orderByRaw('retailers.id');
    }

    /**
     * Execute the job.
     */
    public function handle(): void
    {
        // /** @var \Illuminate\Database\Query\Builder */
        // $query = $this->query();

        $perPage = 2000;
        $totalRecords = $this->query()->count();
        $totalPages = ceil($totalRecords / $perPage);
        $batchUuid = Str::uuid();
        $exportName = class_basename($this);
        $exportDisk = 'temp';
        $exportDirectory = '';

        Log::info('Hesoyam [' . self::class . '] Exporting query', [
            'perPage' => $perPage,
            'totalRecord' => $totalRecords,
            'totalPage' => $totalPages,
            'batchUuid' => $batchUuid,
            'exportName' => $exportName,
            'exportDisk' => $exportDisk,
            'exportDirectory' => $exportDirectory
        ]);

        $export = $this->initializeExport($totalRecords);

        if ($totalRecords <= 0) {
            Log::info('Hesoyam [' . self::class . '] No records to export');
            $export->update([
                'status' => ExportStatus::COMPLETED->value,
                'completed_at' => now(),
            ]);
            return;
        }
        /** @var App\Jobs\ExportToCsv[] */
        $exportJobs = [];
        for ($page = 1; $page <= $totalPages; $page++) {
            $exportJobs[] = new ExportToCsv($this, $page, $perPage, $batchUuid);
        }

        Log::info('Hesoyam [' . self::class . '] Total Export Jobs', [
            'total' => count($exportJobs),
        ]);

        Bus::batch($exportJobs)
            ->progress(function (Batch $batch) use ($export) {
                Log::info('Hesoyam [' . self::class . '] Bus::batch progress', $batch);

                $export->update([
                    'status' => ExportStatus::IN_PROGRESS->value,
                ]);
            })
            ->then(function (Batch $batch) use ($export, $batchUuid, $exportDisk, $exportName, $exportDirectory) {
                Log::info('Hesoyam [' . self::class . '] Bus::batch then', $batch);
                // Collate and upload to disk job
                dispatch(new CollateExportsAndUploadToDisk($export, $batchUuid, $exportName, $exportDisk, $exportDirectory));

                Log::debug("[{$exportName}] Export completed.", [
                    "exportId" => $export->id,
                    "batchId" => $batch->id,
                    "totalJobs" => $batch->totalJobs,
                    "failedJobs" => $batch->failedJobs,
                ]);
            })
            ->allowFailures()
            ->name($exportName)
            ->onQueue('default')
            ->dispatch();
    }

    private function initializeExport(int $totalRecords): Export
    {
        return Export::query()->create([
            'user_id' => $this->user?->id ?? null,
            'user_type' => !empty($this->user) ? get_class($this->user) : null,
            'status' => ExportStatus::PENDING->value,
            'processor' => self::class,
            'file_total_rows' => $totalRecords,
            'started_at' => now(),
        ]);
    }
}
