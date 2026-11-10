# Управление локальным стеком Langfuse.
# Секреты стека лежат в docker/langfuse/.env, benchmark-секреты — в корневом .env.

LANGFUSE_COMPOSE := docker compose -f docker/langfuse/docker-compose.yml --env-file docker/langfuse/.env

.PHONY: langfuse-up langfuse-down langfuse-health langfuse-logs langfuse-reset

langfuse-up:      ## поднять стек и дождаться здоровья
	$(LANGFUSE_COMPOSE) up -d --wait

langfuse-down:    ## остановить, данные сохранить
	$(LANGFUSE_COMPOSE) down

langfuse-health:  ## проверить веб-сервис
	curl -fsS http://localhost:3100/api/public/health

langfuse-logs:    ## хвост логов web и worker
	$(LANGFUSE_COMPOSE) logs --tail=100 langfuse-web langfuse-worker

langfuse-reset:   ## ОПАСНО: удалить стек вместе с томами
	$(LANGFUSE_COMPOSE) down -v
