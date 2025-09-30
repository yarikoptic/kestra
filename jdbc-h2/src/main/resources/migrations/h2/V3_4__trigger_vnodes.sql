-- Add generated columns
ALTER TABLE triggers
    ADD COLUMN "vnode" INT GENERATED ALWAYS AS (JQ_INTEGER("value", '.vNode'));

ALTER TABLE triggers
    ADD COLUMN "locked" BOOLEAN GENERATED ALWAYS AS (JQ_BOOLEAN("value", '.locked'));

-- Indexes
CREATE INDEX IF NOT EXISTS idx_triggers_vnode ON triggers ("vnode");
CREATE INDEX IF NOT EXISTS idx_triggers_locked ON triggers ("locked");

-- Queue trigger event table
CREATE TABLE IF NOT EXISTS queue_trigger_event (
    "offset" BIGINT AUTO_INCREMENT PRIMARY KEY,
    "key" VARCHAR(250),
    "value" JSON NOT NULL,
    "vnode" SMALLINT NOT NULL,
    "created" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

CREATE INDEX IF NOT EXISTS ix_queue_trigger_event_vnode_offset
    ON queue_trigger_event ("vnode", "offset");

-- Queue scheduler event table
CREATE TABLE IF NOT EXISTS queue_scheduler_event (
    "offset" BIGINT AUTO_INCREMENT PRIMARY KEY,
    "key" VARCHAR(250),
    "value" JSON NOT NULL,
    "created" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
