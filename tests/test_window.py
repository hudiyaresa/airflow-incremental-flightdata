import pendulum

from flights_data_pipeline.tasks.window import extract_query, incremental_window, object_name

START = pendulum.datetime(2025, 1, 2, tz="UTC")
END = pendulum.datetime(2025, 1, 3, tz="UTC")


def test_incremental_window_is_half_open_interval():
    params = incremental_window(START, END)

    assert params == {"start": "2025-01-02T00:00:00+00:00", "end": "2025-01-03T00:00:00+00:00"}


def test_incremental_query_uses_parameters_not_literals():
    query = extract_query("bookings", incremental=True)

    assert "bookings.bookings" in query
    assert "%(start)s" in query and "%(end)s" in query
    assert "2025" not in query
    assert "created_at < %(end)s" in query and "updated_at < %(end)s" in query


def test_full_query_reads_whole_table():
    assert extract_query("flights", incremental=False).strip() == "SELECT * FROM bookings.flights;"


def test_object_name_is_keyed_by_interval_start():
    assert object_name("tickets", True, START) == "/temp/tickets-2025-01-02.csv"
    assert object_name("tickets", False, START) == "/temp/tickets.csv"
