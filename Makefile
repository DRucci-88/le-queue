up:
	@docker compose up -d

down:
	@docker compose down

tinker:
	@docker compose exec -it web php artisan tinker

tail:
	@docker compose exec -it laravel.test tail -f storage/logs/laravel.log