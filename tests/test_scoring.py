import sqlite3

from db_utils import recalculate_scores


def _db(rows):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE deals (
            listing_id TEXT PRIMARY KEY, yield_on_cost REAL, population_3mi INTEGER,
            avg_psf REAL, price_per_acre REAL, processed_at TEXT,
            skip_reason TEXT, deal_score REAL
        )""")
    conn.executemany(
        """INSERT INTO deals (listing_id, yield_on_cost, population_3mi, avg_psf,
                              price_per_acre, processed_at, skip_reason)
           VALUES (?,?,?,?,?,?,?)""", rows)
    conn.commit()
    return conn


def _scores(conn):
    return dict(conn.execute("SELECT listing_id, deal_score FROM deals").fetchall())


def test_gates_and_ranking():
    conn = _db([
        ("A", 0.15, 50_000, 1.5, 435_600, "2026-01-01", None),  # strong on all metrics
        ("B", 0.12, 40_000, 1.2, 871_200, "2026-01-01", None),  # weaker on all metrics
        ("C", 0.05, 50_000, 1.5, 435_600, "2026-01-01", None),  # fails YoC >= 0.10 gate
    ])
    recalculate_scores(conn)
    s = _scores(conn)
    assert s["C"] is None                 # gate-failed deal gets NULL score
    assert s["A"] is not None and s["B"] is not None
    assert s["A"] > s["B"]                # dominant deal ranks higher


def test_population_gate():
    conn = _db([
        ("LOW", 0.15, 10_000, 1.5, 435_600, "2026-01-01", None),  # pop < 30k → gated
        ("OK",  0.15, 35_000, 1.2, 435_600, "2026-01-01", None),
    ])
    recalculate_scores(conn)
    s = _scores(conn)
    assert s["LOW"] is None
    assert s["OK"] is not None


def test_skipped_and_unprocessed_deals_excluded():
    conn = _db([
        ("SKIP", 0.15, 50_000, 1.5, 435_600, "2026-01-01", "zoning"),
        ("RAW",  0.15, 50_000, 1.5, 435_600, None, None),
    ])
    recalculate_scores(conn)  # must not raise with zero scorable rows
    s = _scores(conn)
    assert s["SKIP"] is None and s["RAW"] is None
