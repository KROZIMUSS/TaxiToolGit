-- ============================================================
-- user_flows table — per-user stateless flow state for TaxiToolBOT
-- Enables horizontal scaling on Cloud Run (multi-instance webhook mode)
-- ============================================================

CREATE TABLE IF NOT EXISTS public.user_flows (
    user_id         TEXT        PRIMARY KEY,
    flow            TEXT        NOT NULL,           -- e.g. 'create_listing', 'browse', 'edit_listing'
    step            TEXT        NOT NULL,           -- e.g. 'GET_CATEGORY', 'AWAIT_SEARCH_QUERY', 'AWAIT_NEW_DESCRIPTION'
    data            JSONB       NOT NULL DEFAULT '{}',  -- partial listing fields, edit_listing_id, etc.
    last_update_id  BIGINT      NOT NULL DEFAULT 0,     -- for idempotency: skip updates with id <= this
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for periodic cleanup of stale flows (e.g. flows older than 24h)
CREATE INDEX IF NOT EXISTS user_flows_updated_at_idx
    ON public.user_flows (updated_at);

-- Row-Level Security (RLS) — enable if you use Supabase RLS policies
-- ALTER TABLE public.user_flows ENABLE ROW LEVEL SECURITY;
-- Service-role key bypasses RLS, so the bot can read/write freely.

-- ============================================================
-- Optional: Atomic "claim update" function for extra safety
-- Prevents two instances from processing the same update_id.
-- Called via supabase.rpc("claim_update", {...})
-- ============================================================

CREATE OR REPLACE FUNCTION public.claim_update(
    p_user_id      TEXT,
    p_update_id    BIGINT,
    p_flow         TEXT,
    p_step         TEXT,
    p_data         JSONB
)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    affected INT;
BEGIN
    -- Single atomic upsert: insert new row or update only if incoming update_id is greater.
    -- Uses ON CONFLICT DO UPDATE with WHERE clause to prevent older updates overwriting newer.
    INSERT INTO public.user_flows (user_id, flow, step, data, last_update_id, updated_at)
    VALUES (p_user_id, p_flow, p_step, p_data, p_update_id, NOW())
    ON CONFLICT (user_id) DO UPDATE
        SET flow           = EXCLUDED.flow,
            step           = EXCLUDED.step,
            data           = EXCLUDED.data,
            last_update_id = EXCLUDED.last_update_id,
            updated_at     = NOW()
    WHERE user_flows.last_update_id < EXCLUDED.last_update_id;

    GET DIAGNOSTICS affected = ROW_COUNT;
    RETURN affected > 0;
END;
$$;

-- ============================================================
-- Cleanup query (run periodically, e.g. via pg_cron or manually)
-- Removes flows not updated in the last 24 hours
-- ============================================================
-- DELETE FROM public.user_flows WHERE updated_at < NOW() - INTERVAL '24 hours';
