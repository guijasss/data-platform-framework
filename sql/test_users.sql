CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE raw.users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

INSERT INTO raw.users (name, email, active)
VALUES
    ('Alice Johnson', 'alice@example.com', TRUE),
    ('Bob Smith', 'bob@example.com', TRUE),
    ('Carol Davis', 'carol@example.com', FALSE),
    ('David Wilson', 'david@example.com', TRUE),
    ('Eva Brown', 'eva@example.com', FALSE);