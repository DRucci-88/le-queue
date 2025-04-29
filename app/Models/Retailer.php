<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class Retailer extends Model
{
    protected $table = "retailers";

    protected $fillable = [
        'retailer_data_pull_id',
        'account_number',
        'analog_incentive',
        'base_target_met',
        'date_of_visit',
        'streak',
        'filename',
        'fighter_sku',
        'hero_brand',
        'visit_frequency',
        'date_pull',
    ];

    protected $casts = [
        'date_of_visit' => 'date',
        'date_pull' => 'date',
    ];
}
