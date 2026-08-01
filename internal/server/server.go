package server

import (
	"context"
	"fmt"

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
