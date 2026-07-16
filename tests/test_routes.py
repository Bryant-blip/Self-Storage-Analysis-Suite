"""Flask route tests against a temporary SQLite DB."""
import pytest

import db_utils


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(db_utils, "DB", str(tmp_path / "deals.db"))
    db_utils.init_db()
    conn = db_utils.get_db()
    conn.execute("""INSERT INTO deals (listing_id, market, address, city_name,
                    zip_code, processed_at, population_3mi, yield_on_cost)
                    VALUES ('T1','Utah','1 Main St, Lehi, UT 84043','Lehi, UT',
                            '84043','2026-01-01',60000,0.15)""")
    conn.commit()
    conn.close()
    import app
    app.app.config["TESTING"] = True
    return app.app.test_client()


def test_index_serves(client):
    assert client.get("/").status_code == 200


def test_api_city_returns_seeded_deal(client):
    r = client.get("/api/city/Lehi, UT")
    assert r.status_code == 200
    assert "T1" in r.get_data(as_text=True)


def test_api_city_unknown_is_empty_not_error(client):
    r = client.get("/api/city/Nowhere, ZZ")
    assert r.status_code == 200
    assert "T1" not in r.get_data(as_text=True)
