.PHONY: up down reset logs test
up:
	docker compose up --build
down:
	docker compose down
reset:
	docker compose down -v && docker compose up --build
logs:
	docker compose logs -f
test:
	npm test
