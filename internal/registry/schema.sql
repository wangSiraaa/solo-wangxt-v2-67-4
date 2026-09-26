-- Registry schema: packages, immutable versions, consumer declarations,
-- and persisted compatibility reports.

CREATE TABLE IF NOT EXISTS packages (
    id         BIGSERIAL PRIMARY KEY,
    name       TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS versions (
    id             BIGSERIAL PRIMARY KEY,
    package_id     BIGINT NOT NULL REFERENCES packages(id) ON DELETE CASCADE,
    version        TEXT NOT NULL,
    content_hash   BYTEA NOT NULL,
    descriptor_set BYTEA NOT NULL,
    owned_paths    JSONB NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Immutability anchor: the service rejects same-version/different-content
    -- writes; it never updates this row.
    UNIQUE (package_id, version)
);

CREATE TABLE IF NOT EXISTS reports (
    id           BIGSERIAL PRIMARY KEY,
    package_id   BIGINT NOT NULL REFERENCES packages(id) ON DELETE CASCADE,
    base_version TEXT NOT NULL,
    head_version TEXT NOT NULL,
    verdict      TEXT NOT NULL,
    report       JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS reports_package_lookup
    ON reports (package_id, base_version, head_version);

CREATE TABLE IF NOT EXISTS consumers (
    id         BIGSERIAL PRIMARY KEY,
    package_id BIGINT NOT NULL REFERENCES packages(id) ON DELETE CASCADE,
    consumer   TEXT NOT NULL,
    encoding   TEXT NOT NULL CHECK (encoding IN ('wire', 'json', 'both')),
    usages     JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (package_id, consumer)
);

-- Versioned sample corpora.
--
-- Samples are content-addressed per package: the same wire/JSON payload +
-- expectation gets exactly one corpus_samples row no matter how many
-- corpus sets reference it, and re-uploading identical content never
-- creates a second sample.
CREATE TABLE IF NOT EXISTS corpus_samples (
    id          BIGSERIAL PRIMARY KEY,
    package_id  BIGINT NOT NULL REFERENCES packages(id) ON DELETE CASCADE,
    digest      TEXT NOT NULL,
    name        TEXT NOT NULL DEFAULT '',
    message     TEXT NOT NULL,
    encoding    TEXT NOT NULL CHECK (encoding IN ('json', 'wire')),
    data        TEXT NOT NULL,
    expectation JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (package_id, digest)
);

-- A corpus set is one version of one named corpus. Only the single draft
-- tip may change; sealing freezes it forever. Deleting a draft tombstones
-- the row (status='deleted') so version numbers are never reused.
CREATE TABLE IF NOT EXISTS corpus_sets (
    id           BIGSERIAL PRIMARY KEY,
    package_id   BIGINT NOT NULL REFERENCES packages(id) ON DELETE CASCADE,
    corpus       TEXT NOT NULL,
    version      INTEGER NOT NULL,
    status       TEXT NOT NULL CHECK (status IN ('draft', 'sealed', 'deleted')),
    note         TEXT NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    sealed_at    TIMESTAMPTZ,
    UNIQUE (package_id, corpus, version)
);

-- Membership of a set version. Position gives every replay a stable item
-- order; replay_items additionally snapshots the payload, so historical
-- replays stay readable regardless of later draft edits.
CREATE TABLE IF NOT EXISTS corpus_entries (
    set_id    BIGINT NOT NULL REFERENCES corpus_sets(id) ON DELETE CASCADE,
    sample_id BIGINT NOT NULL REFERENCES corpus_samples(id),
    position  INTEGER NOT NULL,
    PRIMARY KEY (set_id, sample_id)
);

CREATE INDEX IF NOT EXISTS corpus_entries_set_idx
    ON corpus_entries (set_id, position);

-- Replay runs. replay_key is the client's idempotency key (or a
-- deterministic derivative of the inputs): one key produces exactly one
-- run, and restarting continues unfinished items instead of redoing them.
CREATE TABLE IF NOT EXISTS replays (
    id             BIGSERIAL PRIMARY KEY,
    package_id     BIGINT NOT NULL REFERENCES packages(id) ON DELETE CASCADE,
    replay_key     TEXT NOT NULL,
    corpus         TEXT NOT NULL,
    corpus_version INTEGER NOT NULL,
    -- Plain FK (no cascade): replays only ever reference sealed sets, and
    -- a sealed set is immutable and undeletable.
    set_id         BIGINT NOT NULL REFERENCES corpus_sets(id),
    schema_version TEXT NOT NULL,
    status         TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    total          INTEGER NOT NULL,
    completed      INTEGER NOT NULL DEFAULT 0,
    summary        JSONB,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (package_id, replay_key)
);

CREATE INDEX IF NOT EXISTS replays_lookup
    ON replays (package_id, corpus, corpus_version);

CREATE TABLE IF NOT EXISTS replay_items (
    replay_id  BIGINT NOT NULL REFERENCES replays(id) ON DELETE CASCADE,
    position   INTEGER NOT NULL,
    digest     TEXT NOT NULL,
    -- Snapshot of the sample as it was when the run started.
    sample     JSONB NOT NULL,
    status     TEXT NOT NULL CHECK (status IN ('pending', 'claimed', 'completed')),
    attempt    INTEGER NOT NULL DEFAULT 0,
    result     JSONB,
    claimed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (replay_id, position)
);
