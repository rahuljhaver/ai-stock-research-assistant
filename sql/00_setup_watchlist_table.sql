-- Setup script for watchlist table
-- Run this manually in your Lakebase Postgres database
--
-- The watchlist table stores user-specific stock symbols they want to track.
-- Each user can have multiple symbols, and each user-symbol pair is unique.

-- Create the watchlist table
CREATE TABLE IF NOT EXISTS watchlist (
    symbol TEXT NOT NULL,                   -- Stock ticker symbol (e.g., 'AMD', 'NVDA')
    email TEXT NOT NULL,                    -- User email (user identifier)
    latest_price NUMERIC,                   -- Optional: last known price for display
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),  -- Last update timestamp
    PRIMARY KEY (symbol, email)             -- Composite key: one entry per user-symbol
);

-- Create index for user lookups (get all symbols for a user)
CREATE INDEX IF NOT EXISTS idx_watchlist_email 
ON watchlist (email);

-- Create index for symbol lookups (get all users watching a symbol)
CREATE INDEX IF NOT EXISTS idx_watchlist_symbol 
ON watchlist (symbol);

-- Verify the table was created
SELECT 
    table_name,
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_name = 'watchlist'
ORDER BY ordinal_position;
