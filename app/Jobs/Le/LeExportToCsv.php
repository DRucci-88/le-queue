<?php

namespace App\Jobs\Le;

use Illuminate\Bus\Batchable;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\Log;
use Spatie\SimpleExcel\SimpleExcelWriter;
use DateTime;

use Illuminate\Database\Eloquent\Model;
use stdClass;

class LeExportToCsv implements ShouldQueue
{
    use Batchable, Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

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
    public $maxExceptions = 2;

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
     * Determine the time at which the job should timeout.
     */
    public function retryUntil(): DateTime
    {
        return now()->addHours(1);
    }

    public function __construct(
        private LeExportProcessor $processor,
        private int $page,
        private int $perPage,
        private string $batchUuid,
    ) {}

    public function displayName(): string
    {
        $displayName = sprintf("%s-%s-%s", self::class, $this->batch()->id, $this->page);
        Log::info(sprintf('[%s] [%s] displayName [%s]', self::class, $this->batchUuid, $displayName), []);
        return $displayName;
    }

    public function handle(): void
    {
        try {

            $items = $this->processor->query()->forPage($this->page, $this->perPage)->get();

            if (empty($items)) return;

            // Leading index is used to make sure that the files are sorted
            $leadingIndex = str_pad($this->page, 5, '0', STR_PAD_LEFT);

            $fileName = "export-{$this->batch()->id}-{$leadingIndex}.csv";
            $csvPath = $this->storagePath($fileName);
            $csvWriter = SimpleExcelWriter::create($csvPath, 'csv');

            Log::info(sprintf('[%s] [%s] file info', self::class, $this->batchUuid), [
                'fileName' => $fileName,
                'csvPath' => $csvPath,
            ]);

            $items->each(function ($item) use ($csvWriter) {
                if ($item instanceof Model) {
                    $item = $item->toArray();
                }

                if ($item instanceof stdClass) {
                    $item = json_decode(json_encode($item), true);
                }

                // Convert arrays inside $data to strings
                foreach ($item as $key => $value) {
                    if (is_array($value)) {
                        $item[$key] = json_encode($value);
                    }
                }

                $csvWriter->addRow($item); // TODO: formatRow
            });

            $csvWriter->close();
        } catch (\Throwable $e) {
            Log::error(sprintf('[%s] [%s] ', self::class, $this->batchUuid), [
                'batchId' => $this->batch()->id,
                'exception' => $e,
            ]);
        }
    }

    protected function storagePath($path = ''): string
    {
        // create temp directory if it doesn't exist
        if (!is_dir(storage_path('app/temp'))) {
            mkdir(storage_path('app/temp'));
        }

        return storage_path('app/temp/' . trim($path, '/'));
    }
}
