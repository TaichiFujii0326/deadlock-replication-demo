#!/bin/bash
set -e

PRIMARY_HOST="mysql-primary"
REPLICA_HOST="mysql-replica"
ROOT_PASS="root"

mysql_primary() {
  mysql -h"$PRIMARY_HOST" -uroot -p"$ROOT_PASS" "$@"
}

mysql_replica() {
  mysql -h"$REPLICA_HOST" -uroot -p"$ROOT_PASS" "$@"
}

echo "=== Waiting for Primary ==="
for i in $(seq 1 30); do
  if mysqladmin ping -h"$PRIMARY_HOST" -uroot -p"$ROOT_PASS" --silent 2>/dev/null; then
    echo "Primary is ready."
    break
  fi
  echo "Waiting for primary... ($i/30)"
  sleep 2
done

echo "=== Waiting for Replica ==="
for i in $(seq 1 30); do
  if mysqladmin ping -h"$REPLICA_HOST" -uroot -p"$ROOT_PASS" --silent 2>/dev/null; then
    echo "Replica is ready."
    break
  fi
  echo "Waiting for replica... ($i/30)"
  sleep 2
done

echo "=== Creating replication user on Primary ==="
mysql_primary <<'SQL'
CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED BY 'replpass';
GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';
FLUSH PRIVILEGES;
SQL

echo "=== Creating schema on Primary ==="
mysql_primary demo <<'SQL'
CREATE TABLE IF NOT EXISTS configs (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id     VARCHAR(50) NOT NULL,
    config_value  TEXT NOT NULL,
    is_deleted    TINYINT(1) NOT NULL DEFAULT 0,
    created_at    TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
    deleted_at    TIMESTAMP(6) NULL,
    INDEX idx_tenant_active (tenant_id, is_deleted)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS traffic_log (
    id         BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id  VARCHAR(50) NOT NULL,
    payload    TEXT NOT NULL,
    created_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
    INDEX idx_tenant (tenant_id)
) ENGINE=InnoDB;
SQL

echo "=== Seeding configs (10000 rows for tenant-A) ==="
for batch in $(seq 0 99); do
  VALUES=""
  START=$((batch * 100 + 1))
  END=$((batch * 100 + 100))
  for i in $(seq $START $END); do
    if [ -n "$VALUES" ]; then
      VALUES="$VALUES,"
    fi
    VALUES="$VALUES('tenant-A', 'seed-config-$i')"
  done
  mysql_primary demo -e "INSERT IGNORE INTO configs (tenant_id, config_value) VALUES $VALUES;"
done
echo "Seeded 10000 config rows."

echo "=== Configuring Replica ==="
mysql_replica <<'SQL'
STOP REPLICA;
RESET REPLICA ALL;
CHANGE REPLICATION SOURCE TO
    SOURCE_HOST='mysql-primary',
    SOURCE_USER='repl',
    SOURCE_PASSWORD='replpass',
    SOURCE_AUTO_POSITION=1,
    GET_SOURCE_PUBLIC_KEY=1;
START REPLICA;
SQL

echo "=== Verifying Replication ==="
sleep 3
for i in $(seq 1 20); do
  STATUS=$(mysql_replica -e "SHOW REPLICA STATUS\G" 2>/dev/null)
  IO_RUNNING=$(echo "$STATUS" | grep "Replica_IO_Running:" | head -1 | awk '{print $NF}')
  SQL_RUNNING=$(echo "$STATUS" | grep "Replica_SQL_Running:" | head -1 | awk '{print $NF}')

  echo "Check $i: IO=$IO_RUNNING SQL=$SQL_RUNNING"

  if [ "$IO_RUNNING" = "Yes" ] && [ "$SQL_RUNNING" = "Yes" ]; then
    echo "Replication is running!"

    # Wait for initial data to replicate
    sleep 3
    COUNT=$(mysql_replica demo -N -e "SELECT COUNT(*) FROM configs;" 2>/dev/null || echo "0")
    echo "Replica configs count: $COUNT"
    echo "=== Replication setup complete ==="
    exit 0
  fi

  sleep 2
done

echo "ERROR: Replication failed to start!"
mysql_replica -e "SHOW REPLICA STATUS\G"
exit 1
