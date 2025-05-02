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

class LeCollateExportsAndUploadToDisk implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public function __construct(
        protected Export $export,
        protected string $batchUuid,
        protected string $exportName,
        protected string $exportDisk,
        protected string $exportDirectory,
        protected string $onQueue,
    ) {
        $this->onQueue($onQueue);
    }

    public function handle(): void
    {
        // Read all file temporary
        $files = $this->getFilesSortedByIndex($this->batchUuid);

        Log::info(sprintf('[%s] [%s] Collating files', self::class, $this->batchUuid), [
            'total' => count($files)
        ]);

        $collatedFileName = $this->exportName . '-' . now()->format('YmdHis') . '.csv';
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
        foreach ($files as $file) {
            unlink($this->storagePath($file));
        }

        Log::info(sprintf('[%s] [%s] Deleting all file', self::class, $this->batchUuid), [
            'files' => $files,
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

        Log::info(sprintf('[%s] [%s] Update export to completed', self::class, $this->batchUuid), [
            'export' => $this->export
        ]);
    }

    protected function storagePath($path = ''): string
    {
        // create temp directory if it doesn't exist
        if (!is_dir(storage_path('app/temp'))) {
            mkdir(storage_path('app/temp'));
        }

        return storage_path('app/temp/' . trim($path, '/'));
    }

    function getFilesSortedByIndex($batchUuid): array
    {
        $allFiles = scandir($this->storagePath());
        $filteredFiles = array_filter($allFiles, function ($file) use ($batchUuid) {
            // Matching the pattern 'export-{uuid}-{index}.csv'
            return preg_match("/export-{$batchUuid}-\d+\.csv$/", $file);
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
}
