<?php

namespace App\Jobs\Le;

use Illuminate\Database\Query\Builder;
use Illuminate\Support\Facades\DB;
use Carbon\Carbon;

class LeRetailerJob2 extends LeExportProcessor
{

    public function query(): Builder
    {
        $this->queueName = 'le-export-2';

        $startDateOfVisit = Carbon::make('2025-01-01');
        $endDateOfVisit = Carbon::make('2025-12-30');

        $startDatePull = Carbon::make('2025-01-01');
        $endDatePull = Carbon::make('2025-12-30');

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
}
