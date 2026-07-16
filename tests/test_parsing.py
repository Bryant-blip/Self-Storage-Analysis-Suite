from crexi.census_pop import parse_city_state_from_address, parse_zip_from_address


def test_full_address():
    assert parse_city_state_from_address("123 Main St, Austin, TX 75001") == ("Austin", "TX")


def test_city_state_only():
    assert parse_city_state_from_address("Lehi, UT") == ("Lehi", "UT")


def test_multiword_city():
    city, state = parse_city_state_from_address("500 W Elm, Salt Lake City, UT 84101")
    assert (city, state) == ("Salt Lake City", "UT")


def test_unparseable():
    assert parse_city_state_from_address("County Road 12") == (None, None)
    assert parse_city_state_from_address("") == (None, None)


def test_zip_takes_last_five_digit_group():
    # Leading 5-digit street numbers must not be mistaken for the ZIP
    assert parse_zip_from_address("10000 Foo St, Austin, TX 78701") == "78701"
    assert parse_zip_from_address("123 Main St, Austin, TX 75001-1234") == "75001"
    assert parse_zip_from_address("no zip here") is None
