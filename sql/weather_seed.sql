-- ===============================================
-- Seed data for weather.cities and weather.locations
-- created_at stored as timezone-aware (Europe/Warsaw)
-- ===============================================

-- Insert cities (without timezone column)
INSERT INTO weather.cities (id, code, name, created_at)
VALUES
    (1, 'PL', 'Warsaw', timezone('Europe/Warsaw', now())),
    (2, 'DE', 'Berlin', timezone('Europe/Warsaw', now())),
    (3, 'US', 'New York', timezone('Europe/Warsaw', now())),
    (4, 'JP', 'Tokyo', timezone('Europe/Warsaw', now())),
    (5, 'GB', 'London', timezone('Europe/Warsaw', now()))
ON CONFLICT (id) DO NOTHING;

-- Insert locations
INSERT INTO weather.locations (id, openweather_id, name, city_id, latitude, longitude, created_at)
VALUES
    (1, 756135, 'Warsaw', 1, 52.2297, 21.0122, timezone('Europe/Warsaw', now())),
    (2, 2950159, 'Berlin', 2, 52.5200, 13.4050, timezone('Europe/Warsaw', now())),
    (3, 5128581, 'New York', 3, 40.7128, -74.0060, timezone('Europe/Warsaw', now())),
    (4, 1850147, 'Tokyo', 4, 35.6895, 139.6917, timezone('Europe/Warsaw', now())),
    (5, 2643743, 'London', 5, 51.5074, -0.1278, timezone('Europe/Warsaw', now()))
ON CONFLICT (id) DO NOTHING;
