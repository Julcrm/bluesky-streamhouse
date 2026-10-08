"""Tests for src.processing.spark.backlog."""

from src.processing.spark.backlog import SparkBacklog, parse_backlog

LINE = 'BLUESKY_BACKLOG {"silver_rows": 720, "gold_hours": 3, "positions": {"silver_posts": "-81"}}'


def test_the_last_backlog_line_wins() -> None:
    messages = ["Running with dbt", LINE.replace("720", "999"), "noise", LINE]
    assert parse_backlog(messages) == SparkBacklog(720, 3, {"silver_posts": "-81"})


def test_no_backlog_line() -> None:
    assert parse_backlog(["Running with dbt", ""]) is None


def test_progress_is_a_changed_position_not_a_bigger_one() -> None:
    """Iceberg snapshot ids are random: a smaller id can be a newer snapshot."""
    before = SparkBacklog(900_000, 0, {"silver_posts": "500"})
    assert SparkBacklog(900_000, 0, {"silver_posts": "-7"}).progressed_from(before)
    assert SparkBacklog(400_000, 0, {"silver_posts": "500"}).progressed_from(before)
    assert not SparkBacklog(900_000, 0, {"silver_posts": "500"}).progressed_from(before)


def test_gold_only_progress() -> None:
    before = SparkBacklog(0, 12, {"silver_posts": "5"})
    assert SparkBacklog(0, 6, {"silver_posts": "5"}).progressed_from(before)
    assert not SparkBacklog(0, 12, {"silver_posts": "5"}).progressed_from(before)
