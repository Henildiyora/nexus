package server

import (
	"context"
	"fmt"
	"regexp"

	"github.com/Henildiyora/nexus/internal/db"
	"github.com/Henildiyora/nexus/internal/pb/statestore"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// StateStoreServer implements the generated pb.StateStoreServiceServer interface.
// It holds a reference to the DB pool so every RPC method can query CockroachDB.
type StateStoreServer struct {
	statestore.UnimplementedStateStoreServiceServer // embedding this gives forwared - cpmpact safety -
	// if add new RPC methods to the .proto later, old servers won't fail to compile
	Pool *pgxpool.Pool
}

// NewStateStoreServer is a constructure - standerd Go pattern instead of exporting the struct raw.
func NewStateStoreServer(pool *pgxpool.Pool) *StateStoreServer {
	return &StateStoreServer{Pool: pool}
}

// Helper: convert Go db.AgentSession -> protobuf Session message
// This is necessary because gRPC only understands the generated pb types,
// not your internal db package's struct. This translation layer is normal
// and important — it keeps DB layer decoupled from API layer.
func toProtoSession(s *db.AgentSession) (*statestore.Session, error) {
	stateStruct, err := structpb.NewStruct(s.State)
	if err != nil {
		return nil, fmt.Errorf("failed to convert state to struct: %w", err)
	}

	return &statestore.Session{
		Id:        s.ID.String(),
		AgentId:   s.AgentID,
		TenantId:  s.TenantID,
		State:     stateStruct,
		CreatedAt: timestamppb.New(s.CreatedAt),
		UpdatedAt: timestamppb.New(s.UpdatedAt),
	}, nil
}

// RPC method implementations
func (s *StateStoreServer) CreateSession(ctx context.Context, req *statestore.CreateSessionRequest) (*statestore.SessionResponse, error) {

	// req.State is a *structpb.Struct - convert to plain Go map for db layer
	stateMap := req.State.AsMap()

	newID, err := db.InsertSession(ctx, s.Pool, req.AgentId, req.TenantId, stateMap)
	if err != nil {
		return nil, fmt.Errorf("create session failed: %w", err)
	}

	// Featch it back, return the full row (with timestemps, etc.)
	session, err := db.GetSession(ctx, s.Pool, newID, req.TenantId)
	if err != nil {
		return nil, fmt.Errorf("fetch after create failed: %w", err)
	}

	protoSession, err := toProtoSession(session)
	if err != nil {
		return nil, err
	}

	return &statestore.SessionResponse{Session: protoSession}, nil
}

func (s *StateStoreServer) GetSession(ctx context.Context, req *statestore.GetSessionRequest) (*statestore.SessionResponse, error) {
	id, err := uuid.Parse(req.Id)
	if err != nil {
		return nil, fmt.Errorf("invalid session id: %w", err)
	}

	session, err := db.GetSession(ctx, s.Pool, id, req.TenantId)
	if err != nil {
		return nil, fmt.Errorf("get session failed: %w", err)
	}

	protoSession, err := toProtoSession(session)
	if err != nil {
		return nil, err
	}

	return &statestore.SessionResponse{Session: protoSession}, nil
}

func (s *StateStoreServer) ListSessionsByTenant(ctx context.Context, req *statestore.ListSessionsByTenantRequest) (*statestore.ListSessionsResponse, error) {
	sessions, err := db.ListSessionsByTenant(ctx, s.Pool, req.TenantId)
	if err != nil {
		return nil, fmt.Errorf("list sessions failed: %w", err)
	}

	protoSessions := make([]*statestore.Session, 0, len(sessions))
	for _, sess := range sessions {
		ps, err := toProtoSession(&sess)
		if err != nil {
			return nil, err
		}
		protoSessions = append(protoSessions, ps)
	}

	return &statestore.ListSessionsResponse{Sessions: protoSessions}, nil
}

func (s *StateStoreServer) UpdateSession(ctx context.Context, req *statestore.UpdateSessionRequest) (*statestore.SessionResponse, error) {
	id, err := uuid.Parse(req.Id)
	if err != nil {
		return nil, fmt.Errorf("invalid session id: %w", err)
	}

	stateMap := req.State.AsMap()

	if err := db.UpdateSession(ctx, s.Pool, id, req.TenantId, stateMap); err != nil {
		return nil, fmt.Errorf("update session failed: %w", err)
	}

	// Featch fresh copy to return the updated row (with new updated_at)
	session, err := db.GetSession(ctx, s.Pool, id, req.TenantId)
	if err != nil {
		return nil, fmt.Errorf("fetch after update failed: %w", err)
	}

	protoSession, err := toProtoSession(session)
	if err != nil {
		return nil, err
	}

	return &statestore.SessionResponse{Session: protoSession}, nil
}

func (s *StateStoreServer) DeleteSession(ctx context.Context, req *statestore.DeleteSessionRequest) (*statestore.DeleteSessionResponse, error) {
	id, err := uuid.Parse(req.Id)
	if err != nil {
		return nil, fmt.Errorf("invalid session id: %w", err)
	}

	if err := db.DeleteSession(ctx, s.Pool, id, req.TenantId); err != nil {
		return nil, fmt.Errorf("delete session failed: %w", err)
	}

	return &statestore.DeleteSessionResponse{Sucess: true}, nil
}

// isValidInterval is a small check before we string-formate `interval`
// into raw SQL. Only allows simple patterns like "5 seconds", "10 minutes", "2 hours".
// This is the guardrail I flagged as a TODO back in Task 1.3 — now we actually add it.
var isValidIntervalPattern = regexp.MustCompile(`^\d+\s+(second|seconds|minute|minutes|hour|hours)$`)

func isValidInterval(interval string) bool {
	return isValidIntervalPattern.MatchString(interval)
}

func (s *StateStoreServer) GetSessionAsOf(ctx context.Context, req *statestore.GetSessionAsOfRequest) (*statestore.SessionResponse, error) {
	id, err := uuid.Parse(req.Id)
	if err != nil {
		return nil, fmt.Errorf("invalid session id: %w", err)
	}

	if !isValidInterval(req.Interval) {
		return nil, fmt.Errorf("invalid interval format: %q", req.Interval)
	}

	session, err := db.GetSessionAsOf(ctx, s.Pool, id, req.TenantId, req.Interval)
	if err != nil {
		return nil, fmt.Errorf("get session as-of failed: %w", err)
	}

	protoSession, err := toProtoSession(session)
	if err != nil {
		return nil, err
	}

	return &statestore.SessionResponse{Session: protoSession}, nil
}
