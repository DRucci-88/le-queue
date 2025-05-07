<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;
use Spatie\MediaLibrary\HasMedia;
use Spatie\MediaLibrary\InteractsWithMedia;
use Illuminate\Database\Eloquent\Relations\MorphTo;

class Export extends Model implements HasMedia
{
    use InteractsWithMedia;

    protected $fillable = [
        'user_id',
        'user_type',
        'filename',
        'status',
        'processor',
        'file_total_rows',
        'started_at',
        'completed_at',
        'batch_id',
        'batch_uuid'
    ];

    protected $casts = [
        'started_at' => 'datetime',
        'completed_at' => 'datetime',
    ];

    public function __construct(array $attributes = [])
    {
        parent::__construct($attributes);
        $this->setTable('exports');
    }

    public function registerMediaCollections(): void
    {
        $this->addMediaCollection('file')->singleFile();
    }

    public function user(): MorphTo
    {
        return $this->morphTo('user');
    }

    public function getProcessorShortNameAttribute(): string
    {
        return class_basename($this->processor);
    }
}