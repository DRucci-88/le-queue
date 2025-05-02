<?php

namespace App\Jobs;

use App\Enums\ExportStatus;
use App\Models\Export;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Spatie\MediaLibrary\MediaCollections\Exceptions\FileDoesNotExist;
use Spatie\MediaLibrary\MediaCollections\Exceptions\FileIsTooBig;
use Spatie\SimpleExcel\SimpleExcelReader;
use Spatie\SimpleExcel\SimpleExcelWriter;
use Illuminate\Support\Facades\Log;

class CollateExportsAndUploadToDisk implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    /**
     * Create a new job instance.
     */
    public function __construct(
        protected Export $export,
        protected string $batchUuid,
        protected string $exportName,
        protected string $exportDisk,
        protected string $exportDirectory
    ) {
        $this->onQueue('default');
    }

    /**
     * Execute the job.
     *
     * @throws FileDoesNotExist
     * @throws FileIsTooBig
     */
    public function handle(): void
    {
        $files = $this->getFilesSortedByIndex($this->batchUuid);

        Log::info('Hesoyam [' . self::class . '] Collating files', [
            'files' => $files,
        ]);

        $collatedFileName = $this->exportName . '-' . now()->format('YmdHis') . '.csv';
        $collatedFilePath = $this->storagePath($collatedFileName);
        $collatedFileWriter = SimpleExcelWriter::create($collatedFilePath);

        Log::info('Hesoyam [' . self::class . '] collated info', [
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
            Log::debug('Hesoyam [' . self::class . '] Deleting file', [
                'file' => $file,
            ]);
            unlink($this->storagePath($file));
        }

        $finalCollateFilePath = "{$this->exportDirectory}/{$collatedFileName}";

        Log::info('Hesoyam [' . self::class . '] Uploading collated file to disk', [
            'disk' => $this->exportDisk,
            'directory' => $this->exportDirectory,
            'path' => $finalCollateFilePath,
        ]);

        // Upload collated file to disk
        $this->export->addMedia($collatedFilePath)
            ->toMediaCollection('file', $this->exportDisk);

        $this->export->update([
            'filename' => $collatedFileName,
            'status' => ExportStatus::COMPLETED->value,
            'completed_at' => now(),
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

    function getFilesSortedByIndex($uuid): array
    {
        $allFiles = scandir($this->storagePath());
        $filteredFiles = array_filter($allFiles, function ($file) use ($uuid) {
            // Matching the pattern 'export-{uuid}-{index}.csv'
            return preg_match("/export-{$uuid}-\d+\.csv$/", $file);
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
