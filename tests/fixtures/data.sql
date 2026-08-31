-- Seed data for the migration fixture.
--
-- Deliberately includes the values that break a naive CSV COPY: embedded
-- delimiters, quotes, backslashes, newlines, tabs, NULL-vs-empty-string,
-- non-ASCII text, and numeric/timestamp boundary values.

INSERT INTO app.customers (email, display_name, home, tags, profile, avatar, balance)
SELECT
    'user' || i || '@example.com',
    'Customer ' || i,
    ROW('Main St ' || i, 'Springfield', LPAD(i::text, 5, '0'))::app.address,
    ARRAY['tag' || (i % 7), 'bulk'],
    jsonb_build_object('n', i, 'nested', jsonb_build_object('flag', i % 2 = 0)),
    decode(lpad(to_hex(i), 8, '0'), 'hex'),
    (i * 1.0001)::numeric(18, 4)
FROM generate_series(1, 500) AS i;

-- Boundary/adversarial rows in the same table.
INSERT INTO app.customers (email, display_name, home, tags, profile, avatar, balance)
VALUES
    ('edge1@example.com', E'quote " and \\ backslash', NULL, NULL, '{}'::jsonb, NULL, 0),
    ('edge2@example.com', E'comma , and newline\nsecond line', NULL, ARRAY[]::text[], 'null'::jsonb, ''::bytea, 99999999999999.9999),
    ('edge3@example.com', E'tab\there', NULL, ARRAY[NULL, 'x'], '{"k": null}'::jsonb, '\x00ff10'::bytea, 0.0001),
    ('edge4@example.com', 'Ünïcödé — 日本語 — emoji 🐘', NULL, ARRAY[''], '[]'::jsonb, NULL, 1);

INSERT INTO app.orders (customer_id, state, note, placed_at)
SELECT c.id,
       (ARRAY['draft', 'placed', 'shipped', 'cancelled'])[1 + (c.id % 4)]::app.order_state,
       CASE WHEN c.id % 5 = 0 THEN NULL ELSE 'note for ' || c.id END,
       CASE WHEN c.id % 3 = 0 THEN NULL ELSE timestamptz '2024-03-01 12:00:00+00' + (c.id || ' minutes')::interval END
FROM app.customers c;

INSERT INTO app.order_lines (order_id, line_no, sku, qty, unit_cents)
SELECT o.id, l, 'SKU-' || o.id || '-' || l, 1 + (l % 3), 100 * l
FROM app.orders o CROSS JOIN generate_series(1, 3) AS l;

INSERT INTO app.audit_log (actor, action, payload)
SELECT CASE WHEN i % 10 = 0 THEN NULL ELSE 'actor' || i END,
       'action-' || i,
       jsonb_build_object('i', i)
FROM generate_series(1, 300) AS i;

-- Rows wide enough to be pushed out to TOAST storage.
INSERT INTO app.documents (title, body, blob)
SELECT 'doc ' || i,
       repeat('lorem ipsum dolor sit amet ', 800),
       decode(repeat('ab', 5000), 'hex')
FROM generate_series(1, 40) AS i;

INSERT INTO app.strict_identity (note) SELECT 'row ' || i FROM generate_series(1, 25) AS i;

INSERT INTO app.events (occurred, kind)
SELECT date '2024-01-01' + (i % 700), 'kind-' || (i % 5)
FROM generate_series(1, 400) AS i;

INSERT INTO app.nasty_strings (id, val, note) VALUES
    (1,  'plain', 'ordinary'),
    (2,  NULL, 'null value'),
    (3,  '', 'empty string, not null'),
    (4,  E'embedded , comma', 'csv delimiter'),
    (5,  E'embedded " quote', 'csv quote'),
    (6,  E'embedded '' apostrophe', 'sql quote'),
    (7,  E'back\\slash', 'backslash'),
    (8,  E'line1\nline2', 'newline'),
    (9,  E'carriage\rreturn', 'cr'),
    (10, E'tab\tsep', 'tab'),
    (11, E'\\N', 'literal backslash-N: the text COPY NULL marker'),
    (12, 'NULL', 'the four-letter word, as data'),
    (13, E'\\.', 'literal backslash-dot: the text COPY terminator'),
    (14, '🐘 emoji and ünïcödé', 'utf8'),
    (15, repeat('x', 100000), 'toasted value');

INSERT INTO reporting.daily_totals (day, order_count, cents)
SELECT date '2024-01-01' + i, i, i * 1000 FROM generate_series(0, 30) AS i;

REFRESH MATERIALIZED VIEW reporting.order_volume;

-- Advance the standalone sequences so they are demonstrably not at their start.
SELECT nextval('app.ticket_seq') FROM generate_series(1, 13);
SELECT nextval('app.cached_seq') FROM generate_series(1, 5);

ANALYZE;
