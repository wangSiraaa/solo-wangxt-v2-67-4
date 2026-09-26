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

-- Versioned sample corpora. Global sample rows are content-addressed within
-- a package; membership rows are copied into every sealed version so draft
-- changes and draft deletion cannot alter historical playback inputs.
CREATE TABLE IF NOT EXISTS corpus_samples (
    id              BIGSERIAL PRIMARY KEY,
    package_id      BIGINT NOT NULL REFERENCES packages(id) ON DELETE CASCADE,
    fingerprint     BYTEA NOT NULL,
    name            TEXT NOT NULL,
    message_name    TEXT NOT NULL,
    encoding        TEXT NOT NULL CHECK (encoding IN ('json', 'wire')),
    data            TEXT NOT NULL,
    expected_result JSONB NOT NULL,
    content_summary TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (package_id, fingerprint)
);

CREATE TABLE IF NOT EXISTS corpus_sets (
    id         BIGSERIAL PRIMARY KEY,
    package_id BIGINT NOT NULL REFERENCES packages(id) ON DELETE CASCADE,
    name       TEXT NOT NULL,
    version    INTEGER NOT NULL CHECK (version > 0),
    status     TEXT NOT NULL CHECK (status IN ('DRAFT', 'SEALED')),
    base_id    BIGINT REFERENCES corpus_sets(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sealed_at  TIMESTAMPTZ,
    UNIQUE (package_id, name, version)
);

CREATE TABLE IF NOT EXISTS corpus_set_samples (
    corpus_set_id BIGINT NOT NULL REFERENCES corpus_sets(id) ON DELETE CASCADE,
    sample_id     BIGINT NOT NULL REFERENCES corpus_samples(id),
    sample_key    TEXT NOT NULL,
    name          TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (corpus_set_id, sample_key),
    UNIQUE (corpus_set_id, sample_id)
);

CREATE TABLE IF NOT EXISTS replays (
    id                   TEXT PRIMARY KEY,
    package_id           BIGINT NOT NULL REFERENCES packages(id) ON DELETE CASCADE,
    corpus_set_id        BIGINT NOT NULL REFERENCES corpus_sets(id),
    schema_version_id    BIGINT NOT NULL REFERENCES versions(id),
    status               TEXT NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETE', 'FAILED')),
    total                INTEGER NOT NULL DEFAULT 0,
    succeeded            INTEGER NOT NULL DEFAULT 0,
    failed               INTEGER NOT NULL DEFAULT 0,
    missing_field_items  INTEGER NOT NULL DEFAULT 0,
    json_diff_items      INTEGER NOT NULL DEFAULT 0,
    lease_expires_at     TIMESTAMPTZ,
    last_error           TEXT NOT NULL DEFAULT '',
    started_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (corpus_set_id, schema_version_id)
);

CREATE INDEX IF NOT EXISTS replays_pending_lookup
    ON replays (status, lease_expires_at);

CREATE TABLE IF NOT EXISTS replay_results (
    replay_id      TEXT NOT NULL REFERENCES replays(id) ON DELETE CASCADE,
    sample_id      BIGINT NOT NULL,
    sample_key     TEXT NOT NULL,
    status         TEXT NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED')),
    error          TEXT NOT NULL DEFAULT '',
    missing_fields JSONB NOT NULL DEFAULT '[]',
    json_diffs     JSONB NOT NULL DEFAULT '[]',
    decoded_json   TEXT NOT NULL DEFAULT '',
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (replay_id, sample_id)
);

CREATE INDEX IF NOT EXISTS replay_results_work_lookup
    ON replay_results (replay_id, status);
