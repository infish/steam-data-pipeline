"""Integration tests against a real, migrated MySQL database.

Skipped unless INTEGRATION_DB=1. Connection settings come from the usual
MYSQL_* environment variables, and MYSQL_DATABASE must end in "_test"
because every test deletes all pipeline rows.
"""
