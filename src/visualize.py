from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from database.mysql_database import get_connection


REPORTS_DIR = Path(__file__).resolve().parent.parent / "reports"


def read_sql(query):
    connection = get_connection()

    try:
        return pd.read_sql(query, connection)
    finally:
        connection.close()


def save_bar_chart(df, x_column, y_column, title, file_name):
    plt.figure(figsize=(12, 7))
    plt.barh(df[x_column], df[y_column])
    plt.title(title)
    plt.xlabel(y_column.replace("_", " ").title())
    plt.ylabel("")
    plt.gca().invert_yaxis()
    plt.tight_layout()
    plt.savefig(REPORTS_DIR / file_name)
    plt.close()


def create_reports():
    REPORTS_DIR.mkdir(exist_ok=True)

    top_owners = read_sql("""
        SELECT name, estimated_owners
        FROM top_games_by_owners
        LIMIT 10
    """)

    top_review_scores = read_sql("""
        SELECT name, review_score_percent
        FROM top_games_by_review_score
        LIMIT 10
    """)

    most_active = read_sql("""
        SELECT name, ccu
        FROM most_active_games_by_ccu
        LIMIT 10
    """)

    publisher_summary = read_sql("""
        SELECT publisher, total_estimated_owners
        FROM publisher_summary
        LIMIT 10
    """)

    save_bar_chart(
        top_owners,
        "name",
        "estimated_owners",
        "Top Games By Estimated Owners",
        "top_games_by_owners.png"
    )

    save_bar_chart(
        top_review_scores,
        "name",
        "review_score_percent",
        "Top Games By Review Score",
        "top_games_by_review_score.png"
    )

    save_bar_chart(
        most_active,
        "name",
        "ccu",
        "Most Active Games By Current Players",
        "most_active_games_by_ccu.png"
    )

    save_bar_chart(
        publisher_summary,
        "publisher",
        "total_estimated_owners",
        "Top Publishers By Estimated Owners",
        "publisher_summary.png"
    )

    print(f"Reports saved to {REPORTS_DIR}")


if __name__ == "__main__":
    create_reports()
