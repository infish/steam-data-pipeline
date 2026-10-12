"""Rebuild the analytics marts from db/transforms/ and test them.

Each transform file rebuilds one mart (DELETE + INSERT) in its own
transaction, so readers see either the old or the new contents and one
failing mart does not block the others. Files run in name order.
"""

import logging
from pathlib import Path

from marts.tests import run_mart_tests


TRANSFORM_DIR = Path(__file__).resolve().parents[2] / "db" / "transforms"


def transform_paths(directory=TRANSFORM_DIR):
    return sorted(Path(directory).glob("*.sql"))


def split_statements(sql):
    """Split a transform file into statements.

    Full-line "--" comments are dropped; a statement ends at a line ending
    in ";". Transform files must not put ";" at the end of a line inside a
    string literal.
    """
    statements = []
    current = []

    for line in sql.splitlines():
        if line.strip().startswith("--"):
            continue

        current.append(line)

        if line.rstrip().endswith(";"):
            statement = "\n".join(current).strip().rstrip(";").strip()

            if statement:
                statements.append(statement)

            current = []

    if "\n".join(current).strip():
        raise ValueError("Transform file ends without a terminating ';'")

    return statements


def build_marts(connection, directory=TRANSFORM_DIR):
    """Run every transform; return [(mart file, error)] for failures."""
    failures = []
    cursor = connection.cursor()

    try:
        for path in transform_paths(directory):
            try:
                for statement in split_statements(path.read_text()):
                    cursor.execute(statement)

                connection.commit()
            except Exception as error:
                connection.rollback()
                logging.exception("Mart transform %s failed", path.name)
                failures.append((path.stem, error))
    finally:
        cursor.close()

    return failures


def build_and_test_marts(connection):
    """Rebuild and test the marts; return (warnings, test results)."""
    failures = build_marts(connection)
    results = run_mart_tests(connection)
    warnings = [
        f"Mart transform {name} failed, previous contents kept: {error}"
        for name, error in failures
    ]
    warnings += [
        f"Mart test {result['check_name']} failed on {result['target']}:"
        f" observed {result['observed_value']}, expected"
        f" {result['threshold']}"
        for result in results
        if result["status"] == "fail"
    ]

    for result in results:
        if result["status"] == "warn":
            logging.info(
                "Mart test %s on %s: %s",
                result["check_name"],
                result["target"],
                result["observed_value"]
            )

    return warnings, results
