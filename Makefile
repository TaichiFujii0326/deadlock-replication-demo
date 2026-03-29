.PHONY: demo race-demo up wait run down logs status clean

demo: up wait run

up:
	docker compose up -d

wait:
	@echo "Waiting for replication-init to complete..."
	@while [ "$$(docker inspect -f '{{.State.Status}}' replication-init 2>/dev/null)" != "exited" ]; do \
		sleep 2; \
		echo "  ...still initializing"; \
	done
	@EXIT_CODE=$$(docker inspect -f '{{.State.ExitCode}}' replication-init 2>/dev/null); \
	if [ "$$EXIT_CODE" != "0" ]; then \
		echo "ERROR: replication-init failed (exit code $$EXIT_CODE)"; \
		docker logs replication-init; \
		exit 1; \
	fi
	@echo "Replication ready."
	@sleep 3

run:
	go run ./cmd/demo/...

race-demo: up wait
	go run ./cmd/race-demo/

down:
	docker compose down -v

logs:
	docker compose logs -f

status:
	@docker exec mysql-replica mysql -uroot -proot -e "SHOW REPLICA STATUS\G"

clean: down
	rm -rf primary-data replica-data
