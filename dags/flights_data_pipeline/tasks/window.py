"""Shared helpers so extract and load agree on the incremental window and the MinIO object name."""


def incremental_window(data_interval_start, data_interval_end):
    """Return query parameters for rows created or updated inside [start, end)."""
    return {
        "start": data_interval_start.isoformat(),
        "end": data_interval_end.isoformat(),
    }


def extract_query(table_name, incremental):
    if not incremental:
        return f"SELECT * FROM bookings.{table_name};"

    return f"""
        SELECT * FROM bookings.{table_name}
        WHERE (created_at >= %(start)s AND created_at < %(end)s)
           OR (updated_at >= %(start)s AND updated_at < %(end)s);
    """


def object_name(table_name, incremental, data_interval_start):
    if not incremental:
        return f"/temp/{table_name}.csv"
    return f"/temp/{table_name}-{data_interval_start.strftime('%Y-%m-%d')}.csv"
