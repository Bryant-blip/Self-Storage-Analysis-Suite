from crexi.census_pop import haversine, _bbox_candidates


def test_haversine_known_distance():
    # NYC to LA is ~2,445 miles great-circle
    d = haversine(40.7128, -74.0060, 34.0522, -118.2437)
    assert 2400 < d < 2500


def test_haversine_zero_distance():
    assert haversine(40.0, -111.0, 40.0, -111.0) == 0.0


def test_bbox_prefilter_never_drops_in_radius_points():
    # The bbox filter is a pure prefilter: every place within the radius
    # must survive it (it may pass extras; haversine culls those later).
    lat, lng, radius = 40.39, -111.85, 3.0
    places = {}
    step = 0.01  # ~0.7 mi grid around the subject
    for i in range(-10, 11):
        for j in range(-10, 11):
            places[(str(i), str(j))] = {"lat": lat + i * step, "lng": lng + j * step}

    in_radius = {k for k, v in places.items()
                 if haversine(lat, lng, v["lat"], v["lng"]) <= radius}
    survived = {k for k, _ in _bbox_candidates(places, lat, lng, radius)}
    assert in_radius <= survived
