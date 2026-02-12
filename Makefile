# =============================================================================
# Crypto Lambda Analytics — Makefile
# =============================================================================

.PHONY: help up up-full down down-v status logs batch seed test

COMPOSE_BATCH := docker compose -f docker-compose.yml
COMPOSE_FULL  := docker compose -f docker-compose.yml -f docker-compose.streaming.yml

# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------

help: ## Show available commands
	@echo ""
	@echo "Crypto Lambda Analytics"
	@echo "======================"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""

# ---------------------------------------------------------------------------
# Infrastructure — Phase 1 (Batch Only)
# ---------------------------------------------------------------------------

up: ## Start batch pipeline (HDFS, Spark, Airflow, Hive, Grafana)
	@echo "Starting batch pipeline services..."
	$(COMPOSE_BATCH) up -d
	@echo ""
	@echo "Batch pipeline starting. Run 'make status' to check."
	@echo ""
	@echo "  HDFS NameNode:   http://localhost:9870"
	@echo "  HDFS DataNode:   http://localhost:9864"
	@echo "  Spark Master:    http://localhost:8080"
	@echo "  Spark Worker:    http://localhost:8081"
	@echo "  Airflow:         http://localhost:8085"
	@echo "  Grafana:         http://localhost:3000"
	@echo ""
	@echo "  Credentials are in your .env file (see .env.example)"
	@echo ""

# ---------------------------------------------------------------------------
# Infrastructure — Full Stack (Batch + Streaming)
# ---------------------------------------------------------------------------

up-full: ## Start full stack (batch + Kafka, Flink, Redis)
	@echo "Starting full Lambda Architecture..."
	$(COMPOSE_FULL) up -d
	@echo ""
	@echo "  --- Batch Layer ---"
	@echo "  HDFS NameNode:   http://localhost:9870"
	@echo "  Spark Master:    http://localhost:8080"
	@echo "  Airflow:         http://localhost:8085"
	@echo ""
	@echo "  --- Streaming Layer ---"
	@echo "  Kafka UI:        http://localhost:9021"
	@echo "  Flink Dashboard: http://localhost:8083"
	@echo "  Redis Insight:   http://localhost:5540"
	@echo ""
	@echo "  --- Shared ---"
	@echo "  Grafana:         http://localhost:3000"
	@echo ""
	@echo "  Credentials are in your .env file (see .env.example)"
	@echo ""

down: ## Stop batch services
	$(COMPOSE_BATCH) down

down-full: ## Stop all services (batch + streaming)
	$(COMPOSE_FULL) down

down-v: ## Stop batch services + remove volumes
	$(COMPOSE_BATCH) down -v

down-full-v: ## Stop all services + remove volumes (full reset)
	$(COMPOSE_FULL) down -v

restart: down up ## Restart batch services

status: ## Show batch service status
	$(COMPOSE_BATCH) ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

status-full: ## Show all service status
	$(COMPOSE_FULL) ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

logs: ## Tail batch service logs
	$(COMPOSE_BATCH) logs -f --tail=50

# ---------------------------------------------------------------------------
# Individual Service Logs
# ---------------------------------------------------------------------------

logs-spark: ## Tail Spark Master logs
	docker logs -f spark-master --tail=50

logs-airflow: ## Tail Airflow logs
	docker logs -f airflow-webserver --tail=50

logs-hdfs: ## Tail HDFS NameNode logs
	docker logs -f namenode --tail=50

# ---------------------------------------------------------------------------
# Data Operations
# ---------------------------------------------------------------------------

seed: ## Generate and load sample data into HDFS
	@echo "Generating sample data and uploading to HDFS..."
	docker exec spark-master /opt/spark/bin/spark-submit \
		--master local[*] \
		--conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
		/opt/scripts/seed_data.py
	@echo "Sample data loaded."

hdfs-ls: ## List HDFS data directories
	docker exec namenode hdfs dfs -ls -R /data/ 2>/dev/null | head -60

hdfs-usage: ## Show HDFS storage usage
	docker exec namenode hdfs dfs -du -h /data/

hdfs-count: ## Count files per zone
	@echo "=== Raw Zone ==="
	@docker exec namenode hdfs dfs -count /data/raw/ 2>/dev/null || echo "  (empty)"
	@echo "=== Staging Zone ==="
	@docker exec namenode hdfs dfs -count /data/staging/ 2>/dev/null || echo "  (empty)"
	@echo "=== Warehouse Zone ==="
	@docker exec namenode hdfs dfs -count /data/warehouse/ 2>/dev/null || echo "  (empty)"

# ---------------------------------------------------------------------------
# Batch Pipeline
# ---------------------------------------------------------------------------

batch: ## Run full batch pipeline (all Spark jobs)
	@echo "=== Running batch pipeline ==="
	@echo ""
	@echo "[1/3] Portfolio snapshot..."
	docker exec spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.sql.adaptive.enabled=true \
		--conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
		/opt/spark-jobs/daily_portfolio_snapshot.py
	@echo ""
	@echo "[2/3] Reward summary..."
	docker exec spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.sql.adaptive.enabled=true \
		--conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
		/opt/spark-jobs/reward_summary.py
	@echo ""
	@echo "[3/3] Data quality checks..."
	docker exec spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
		/opt/spark-jobs/data_quality_checks.py
	@echo ""
	@echo "=== Batch pipeline complete ==="

batch-portfolio: ## Run only the portfolio snapshot job
	docker exec spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.sql.adaptive.enabled=true \
		--conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
		/opt/spark-jobs/daily_portfolio_snapshot.py

batch-rewards: ## Run only the reward summary job
	docker exec spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.sql.adaptive.enabled=true \
		--conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
		/opt/spark-jobs/reward_summary.py

# ---------------------------------------------------------------------------
# Spark Shell (interactive)
# ---------------------------------------------------------------------------

spark-shell: ## Open PySpark interactive shell connected to cluster
	docker exec -it spark-master /opt/spark/bin/pyspark \
		--master spark://spark-master:7077 \
		--conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000

# ---------------------------------------------------------------------------
# Testing
# ---------------------------------------------------------------------------

test: ## Run all tests
	pytest tests/ -v --tb=short

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

clean: ## Remove Python caches and build artifacts
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true

clean-all: clean down-v ## Full cleanup: artifacts + services + volumes
	@echo "Full cleanup complete."