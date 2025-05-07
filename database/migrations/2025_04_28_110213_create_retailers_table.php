<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Run the migrations.
     */
    public function up(): void
    {
        Schema::create('retailers', function (Blueprint $table) {
            $table->id(); // bigint unsigned auto-increment primary key
            
            $table->string('filename', 50);
            $table->string('account_number', 7);
            $table->integer('visit_frequency')->nullable();
            $table->string('hero_brand', 2)->nullable();
            $table->string('fighter_sku', 7)->nullable();
            $table->integer('streak');
            $table->timestamp('date_of_visit');
            $table->string('base_target_met', 3);
            $table->timestamp('date_pull');
            $table->integer('analog_incentive');

            $table->timestamps(); // created_at and updated_at (nullable timestamps)

            // Indexes
            $table->index(['account_number', 'date_of_visit', 'filename'], 'retailers_account_date_filename_index');
            $table->index(['date_of_visit', 'date_pull'], 'retailers_date_index');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('retailers');
    }
};
