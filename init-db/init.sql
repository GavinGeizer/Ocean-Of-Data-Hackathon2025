-- Create and enable the PostGIS extension for geospatial data
CREATE EXTENSION IF NOT EXISTS postgis;

-- Create and enable the TimescaleDB extension for time-series handling
-- Note: This is required only on standard PostgreSQL; cloud versions may have it pre-installed.
CREATE EXTENSION IF NOT EXISTS timescaledb;


-- -------------------------------------------------------------
-- Create the main boat position table
-- -------------------------------------------------------------
CREATE TABLE boat_positions (
    -- Primary ID for individual readings
    id BIGSERIAL PRIMARY KEY,
    -- Time is the most important field for a time-series DB
    reported_at TIMESTAMPTZ NOT NULL,
    -- Unique ID for the boat
    boat_id INT NOT NULL,
    -- PostGIS GEOGRAPHY type for accurate location data
    location GEOGRAPHY(POINT, 4326) NOT NULL
);

-- -------------------------------------------------------------
-- Convert the table into a TimescaleDB Hypertable
-- -------------------------------------------------------------
-- This automatically partitions the table by time for performance!
SELECT create_hypertable('boat_positions', 'reported_at', if_not_exists => TRUE);

-- -------------------------------------------------------------
-- Create Indexes
-- -------------------------------------------------------------
-- Index on boat_id for filtering a single boat's history
CREATE INDEX IF NOT EXISTS idx_boat_positions_boat_id ON boat_positions (boat_id, reported_at DESC);

-- Spatial index on the location column for fast geo-queries
CREATE INDEX IF NOT EXISTS idx_boat_positions_location ON boat_positions USING GIST (location);
s