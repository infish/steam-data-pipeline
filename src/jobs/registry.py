"""All pipeline jobs, in the order a full manual run executes them."""

from jobs.catalog import CATALOG_JOB
from jobs.players import PLAYERS_JOB
from jobs.refresh import REFRESH_JOB
from jobs.reviews import REVIEWS_JOB


# The catalog runs first: player and review rows need a games row. The
# refresh comes after the players job, which may add games from the charts.
JOBS = {
    job.name: job
    for job in (CATALOG_JOB, PLAYERS_JOB, REFRESH_JOB, REVIEWS_JOB)
}
