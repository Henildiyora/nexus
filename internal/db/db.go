package db

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// AgenSession mirrors a row in the agent_sessions table.
// This is the struct pass around in Go code instead of raw SQL rows.
type AgentSession struct {
	ID        uuid.UUID
	AgentID   string
	TenantID  string
	State     map[string]interface{} // decoded JSONB
	CreatedAt time.Time
	UpdatedAt time.Time
}

func ConnectDB(ctx context.Context, connString string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}
	return pool, nil
}

// InitSchema creates the agent_sessions table if it does't exist.
// NOTE: gen_random_uuid() -> random primary keys, NOT sequential.
// Why: CockroachDB distributes rows across nodes by key range. Sequential IDs
// (1,2,3...) all land in the same range at the same time = "hotspot" = one node
// gets hammered while others sit idle. Random UUIDs spread writes evenly across
// the cluster. This is THE classic CockroachDB schema-design rule.
func InitSchema(ctx context.Context, pool *pgxpool.Pool) error {
	query := `
	CREATE TABLE IF NOT EXISTS agent_sessions (
		id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		agent_id   STRING NOT NULL,
		tenant_id  STRING NOT NULL,
		state      JSONB NOT NULL DEFAULT '{}',
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
	);
	`

	_, err := pool.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}

	return nil
}

// InsertSession inserts a new agent session and returns its generated UUID.
// We marshal the Go map -> JSON bytes -> pass as the JSONB value.
// pgx handles []byte -> JSONB conversion automatically.
func InsertSession(ctx context.Context, pool *pgxpool.Pool, agentID string, tenantID string, state map[string]interface{}) (uuid.UUID, error) {

	stateJSON, err := json.Marshal(state)
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to marshal state: %w", err)
	}

	var newID uuid.UUID
	query := `
	INSERT INTO agent_sessions (agent_id, tenant_id, state)
	VALUES ($1,$2,$3)
	RETURNING id;
	`

	// RETURNING id lets us get the DB-generated UUID back in the same round trip
	// instead of doing a separate SELECT after inserting.
	err = pool.QueryRow(ctx, query, agentID, tenantID, stateJSON).Scan(&newID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to insert session: %w", err)
	}
	return newID, nil
}

// GetSession now requires tenantID too — you can ONLY fetch a session
// if you know both its ID and its correct tenant. This is a cheap but
// real first line of defense against cross-tenant data leaks.
func GetSession(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID, tenantID string) (*AgentSession, error) {
	query := `
		SELECT 	id, agent_id, tenant_id, state, created_at, updated_at
		FROM agent_sessions
		WHERE id = $1 AND tenant_id = $2;
	`

	var s AgentSession
	var stateJSON []byte // scan JSONB into raw bytes first, then unmarshal

	err := pool.QueryRow(ctx, query, id, tenantID).Scan(
		&s.ID, &s.AgentID, &s.TenantID, &stateJSON, &s.CreatedAt, &s.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get session: %w", err)
	}

	if err := json.Unmarshal(stateJSON, &s.State); err != nil {
		return nil, fmt.Errorf("failed to unmarshal state: %w", err)
	}

	return &s, nil
}

// ListSessionsByTenant returns ALL sessions beloging to one tenant,
// across all of the tenant's agents. Useful for admin/debug views.
func ListSessionsByTenant(ctx context.Context, pool *pgxpool.Pool, tenantID string) ([]AgentSession, error) {
	query := `
	SELECT id, agent_id, tenant_id, state, created_at, updated_at
	FROM agent_sessions
	WHERE tenant_id = $1
	ORDER BY created_at DESC;
	`

	rows, err := pool.Query(ctx, query, tenantID)
	if err != nil {
		return nil, fmt.Errorf("failed to list sessions: %w", err)
	}
	defer rows.Close() // ALWAYS close rows — leaking these exhausts pool connections

	var sessions []AgentSession
	for rows.Next() {
		var s AgentSession
		var stateJSON []byte
		if err := rows.Scan(&s.ID, &s.AgentID, &s.TenantID, &stateJSON, &s.CreatedAt, &s.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan roe: %w", err)
		}

		if err := json.Unmarshal(stateJSON, &s.State); err != nil {
			return nil, fmt.Errorf("failed to unmarshal state: %w", err)
		}
		sessions = append(sessions, s)
	}

	// rows.Err() catches errors that happened DURING interation
	// (e.g. connection dropped mid-scan) - easy to forget, easy bug source.
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row interation error: %w", err)
	}

	return sessions, nil
}

// UpdateSession overwrite the state blob for an existing session and
// bumps updated_at. Notice created_at is untouched - we only SET the
// columns the should actually change.
func UpdateSession(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID, tenantID string, newState map[string]interface{}) error {
	stateJSON, err := json.Marshal(newState)
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	query := `
	UPDATE agent_sessions
	SET state = $1, updated_at = now()
	WHERE id = $2 AND tenant_id = $3;
	`

	// Exec (not QueryRow) because UPDATE doesn't return roes by default.
	tag, err := pool.Exec(ctx, query, stateJSON, id, tenantID)
	if err != nil {
		return fmt.Errorf("failed to update session: %w", err)
	}

	// tag.RowsAffected() tells us if the WHERE clause actually matched anything.
	// If 0 rows were affected, either the id doesn't exist OR the tenantID was
	// wrong — same tenant-isolation protection pattern as GetSession.
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("no session found with id=%s for tenant=%s", id, tenantID)
	}

	return nil
}

// DeleteSession hard-deletes a session row. Tenant-scoped like everything else.
func DeleteSession(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID, tenantID string) error {
	query := `
		DELETE FROM agent_sessions
		WHERE id = $1 AND tenant_id = $2;`

	tag, err := pool.Exec(ctx, query, id, tenantID)
	if err != nil {
		return fmt.Errorf("failed to delete session: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return fmt.Errorf("no session found with id=%s for tenant=%s", id, tenantID)
	}

	return nil
}

// GetSessionAsOf fetches what a session's state looked like at a past point in time,
// using CockroachDB's native time-travel (AS OF SYSTEM TIME) — no audit table needed.
// `interval` is a Postgres-style interval string, e.g. "10 seconds", "5 minutes".
func GetSessionAsOf(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID, tenantID string, interval string) (*AgentSession, error) {
	// NOTE: AS OF SYSTEM TIME can't be parameterized with $N placeholders in all
	// drivers/positions the same way normal values can — safest reliable approach
	// is fmt.Sprintf here since `interval` is a trusted, internally-controlled string,
	// not raw user input passed straight through in a real API yet.
	query := fmt.Sprintf(`
		SELECT id, agent_id, tenant_id, state, created_at, updated_at
		FROM agent_sessions AS OF SYSTEM TIME '-%s'
		WHERE id = $1 AND tenant_id = $2;
	`, interval)

	var s AgentSession
	var stateJSON []byte

	err := pool.QueryRow(ctx, query, id, tenantID).Scan(
		&s.ID, &s.AgentID, &s.TenantID, &stateJSON, &s.CreatedAt, &s.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get historical session: %w", err)
	}

	return &s, nil
}
