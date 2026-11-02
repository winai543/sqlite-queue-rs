-- Load the library before running this file (see README.md).
PRAGMA journal_mode=WAL;
PRAGMA busy_timeout=5000;
SELECT queue_init();
SELECT queue_version();
SELECT queue_push('email', '{"to":"alice@example.com"}', 0,
    '{"priority":10,"max_attempts":3,"dedup_key":"welcome:alice","retry_base_seconds":2}');
-- Same queue/key returns the original ID and preserves its payload/options.
SELECT queue_push('email', 'ignored duplicate', 0, '{"dedup_key":"welcome:alice"}');
SELECT queue_push_batch('email', '[{"payload":"bob"},{"payload":"later","delay_seconds":60}]');

SELECT queue_pause('email');
SELECT queue_claim('email'); -- NULL while paused
SELECT queue_resume('email');
SELECT queue_claim('email', 30, 'sql-demo');
-- Apps keep the claim JSON in memory. For this SQL-only demo, copy its receipt.
CREATE TEMP TABLE claimed AS SELECT id,receipt FROM queue_jobs WHERE worker='sql-demo';
SELECT queue_heartbeat(id,receipt,60) FROM claimed;
SELECT queue_ack(id,receipt) FROM claimed;
DROP TABLE claimed;

SELECT queue_claim('email', 30, 'sql-demo');
CREATE TEMP TABLE claimed AS SELECT id,receipt FROM queue_jobs WHERE worker='sql-demo';
-- permanent=1 sends the job straight to dead-letter.
SELECT queue_fail(id,receipt,'bad recipient',1) FROM claimed;
SELECT queue_redrive(id,10) FROM claimed;
DROP TABLE claimed;
SELECT queue_stats('email');
SELECT queue_list('email', 'pending', 10);
