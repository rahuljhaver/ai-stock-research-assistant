CREATE TABLE research_notes (
    id BIGSERIAL PRIMARY KEY,
    email TEXT NOT NULL,
    symbol TEXT,
    title TEXT NOT NULL,
    note TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE analysis_reports (
    id BIGSERIAL PRIMARY KEY,
    email TEXT NOT NULL,
    symbol TEXT,
    title TEXT NOT NULL,
    analysis TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);