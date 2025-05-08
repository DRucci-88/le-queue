<?php

namespace App\Jobs\Le;

use App\Enums\ExportStatus;
use App\Models\Export;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\Log;
use Spatie\SimpleExcel\SimpleExcelReader;
use Spatie\SimpleExcel\SimpleExcelWriter;
use Illuminate\Support\Facades\Bus;
use DateTime;
use Illuminate\Queue\ManuallyFailedException;
use Throwable;

class LeCollateExportsAndUploadToDisk implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    /**
     * The number of times the job may be attempted.
     *
     * @var int
     */
    public $tries = 6;

    /**
     * The maximum number of unhandled exceptions to allow before failing.
     *
     * @var int
     */
    public $maxExceptions = 4;

    /**
     * The number of seconds the job can run before timing out.
     *
     * @var int
     */
    public $timeout = 90;

    /**
     * Indicate if the job should be marked as failed on timeout.
     *
     * @var bool
     */
    public $failOnTimeout = true;

    /**
     * The number of seconds to wait before retrying the job.
     *
     * @var int
     */
    public $backoff = 3;

    /**
     * Determine the time at which the job should timeout.
     */
    public function retryUntil(): DateTime
    {
        return now()->addHours(1);
    }

    public function __construct(
        protected string $queueName,
        protected Export $export,
        protected string $batchUuid,
        protected string $batchId,
        protected string $exportName,
        protected string $exportDisk,
        protected string $exportDirectory,
        protected int $totalJobs,
    ) {
        $this->onQueue($queueName);
    }

    public function displayName(): string
    {
        $displayName = sprintf("%s-%s", self::class, $this->batchId);
        Log::info(sprintf('[%s] [%s] displayName [%s]', self::class, $this->batchUuid, $displayName), []);
        return $displayName;
    }

    public function handle(): void
    {
        try {
            
            // Read all file temporary
            $files = $this->getFilesSortedByIndex($this->batchId);

            $this->validateTotalFileAndJob($files);

            Log::info('HESOYAM', []);

            // throw new \Exception('MENJADI GILAK');

            $collatedFileName = $this->exportName . '_' . now()->format('Y-m-d_H:i:s') . '.csv';
            $collatedFilePath = $this->storagePath($collatedFileName);
            $collatedFileWriter = SimpleExcelWriter::create($collatedFilePath);

            Log::info(sprintf('[%s] [%s] Collating info', self::class, $this->batchUuid), [
                'collatedFileName' => $collatedFileName,
                'collatedFilePath' => $collatedFilePath,
                'collatedFileWriter' => $collatedFileWriter
            ]);

            foreach ($files as $file) {
                $fileRows = SimpleExcelReader::create($this->storagePath($file))->getRows();
                $collatedFileWriter->addRows($fileRows);
            }

            $collatedFileWriter->close();

            // Delete all files
            $this->deleteAllFile($files);

            Log::info(sprintf('[%s] [%s] Deleting all file', self::class, $this->batchUuid), [
                'files count' => count($files),
            ]);

            $finalCollateFilePath = "{$this->exportDirectory}/{$collatedFileName}";

            // Upload collated file to disk
            $this->export->addMedia($collatedFilePath)
                ->toMediaCollection('file', $this->exportDisk);

            Log::info(sprintf('[%s] [%s] Uploaded collated file to disk', self::class, $this->batchUuid), [
                'disk' => $this->exportDisk,
                'directory' => $this->exportDirectory,
                'path' => $finalCollateFilePath,
            ]);

            $this->export->update([
                'filename' => $collatedFileName,
                'status' => ExportStatus::COMPLETED->value,
                'completed_at' => now(),
            ]);

            Log::info(sprintf('[%s] [%s] Update export completed', self::class, $this->batchUuid), [
                'export' => $this->export
            ]);
        } catch (\Throwable $e) {
            // Log::error(sprintf('[%s] [%s] handle catch', self::class, $this->batchUuid), [
            //     'batchId' => $this->batchId,
            //     'exception' => $e,
            // ]);
            throw $e;
        }
    }

    public function failed(?Throwable $e): void
    {
        Log::error(sprintf('[%s] [%s] Export Failed', self::class, $this->batchUuid), [
            'attempts' => $this->attempts(),
            'batchId' => $this->batchId,
            'exportId' => $this->export->id,
            'exception' => $e,
        ]);

        if (
            $e instanceof ManuallyFailedException
            // || $this->attempts() >= $this->tries
            // || $e instanceof \Illuminate\Queue\MaxAttemptsExceededException
            // || $e instanceof \Illuminate\Queue\TimeoutExceededException
        ) {
            $this->export->update([
                'status' => ExportStatus::FAILED->value
            ]);

            $parentBatch = Bus::findBatch($this->batchId);
            $parentBatch->cancel();

            // Cleaning up
            Log::error(sprintf('[%s] [%s] Perform cleaning csv after Export Failed', self::class, $this->batchUuid), []);
            $files = $this->getFilesSortedByIndex($this->batchId);
            $this->deleteAllFile($files);
        }
    }

    public function storagePath($path = ''): string
    {
        // create temp directory if it doesn't exist
        if (!is_dir(storage_path('app/temp'))) {
            mkdir(storage_path('app/temp'));
        }

        return storage_path('app/temp/' . trim($path, '/'));
    }

    public function getFilesSortedByIndex($batchId): array
    {
        $allFiles = scandir($this->storagePath());
        $filteredFiles = array_filter($allFiles, function ($file) use ($batchId) {
            // Matching the pattern 'export-{uuid}-{index}.csv'
            return preg_match("/export-{$batchId}-\d+\.csv$/", $file);
        });

        usort($filteredFiles, function ($a, $b) {
            // Extracting index from filename
            preg_match("/export-[^-]+-(\d+)\.csv$/", $a, $matchesA);
            $indexA = $matchesA[1] ?? 0;
            preg_match("/export-[^-]+-(\d+)\.csv$/", $b, $matchesB);
            $indexB = $matchesB[1] ?? 0;

            return $indexA <=> $indexB;
        });

        return $filteredFiles;
    }

    public function validateTotalFileAndJob(array $files): void
    {

        // $this->totalJobs = $this->totalJobs + 1; // TODO nanti di hapus, buat testing doang

        Log::info(sprintf('[%s] [%s] Collating files compare to jobs', self::class, $this->batchUuid), [
            'filteredFiles' => count($files),
            'totalJobs' => $this->totalJobs,
        ]);

        if (count($files) === $this->totalJobs) return;

        $exception = new ManuallyFailedException('There are ExportToCsv job fail, difference file [' . abs($this->totalJobs - count($files)) . ']');

        $this->fail($exception); // This will stop the job ignoring $tries or $maxException
        throw $exception;
    }

    public function deleteAllFile(array $files)
    {
        // Delete all files
        foreach ($files as $file) {
            unlink($this->storagePath($file));
        }
    }
}
