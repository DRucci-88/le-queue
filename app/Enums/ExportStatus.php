<?php

namespace App\Enums;

enum ExportStatus: string
{
    case PENDING = "Pending";
    case IN_PROGRESS = "In Progress";
    case FAILED = "Failed";
    case COMPLETED = "Completed";
    case STOPPING = "Stopping";
    case STOPPED = "Stopped";
}