#!/bin/bash
set -euo pipefail

mysql --protocol=socket \
    -uroot \
    -p"${MYSQL_ROOT_PASSWORD}" <<SQL
CREATE USER IF NOT EXISTS '${DASHBOARD_DB_USER}'@'%'
    IDENTIFIED BY '${DASHBOARD_DB_PASSWORD}';

GRANT SELECT, SHOW VIEW
    ON \`${MYSQL_DATABASE}\`.*
    TO '${DASHBOARD_DB_USER}'@'%';
SQL