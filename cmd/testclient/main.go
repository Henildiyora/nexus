package main

import (
	"context"
	"fmt"
	"log"
	"time"

	statestorepb "github.com/Henildiyora/nexus/internal/pb/statestore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"
)

func main() {
	// Connect to the running gRPC server.
	// insecure.NewCredentials() = no TLS, fine for localhost dev — NEVER use in production.
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	// This is the generated CLIENT stub — the counterpart to the server interface
	// you implemented. Calling its methods sends real RPCs over the network.
	client := statestorepb.NewStateStoreServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. CreateSession
	stateStruct, err := structpb.NewStruct(map[string]interface{}{
		"note":  "created via grpc test client",
		"stage": 1,
	})
	if err != nil {
		log.Fatalf("failed to build struct: %v", err)
	}

	createResp, err := client.CreateSession(ctx, &statestorepb.CreateSessionRequest{
		AgentId:  "agent-grpc-test",
		TenantId: "tenant-henil",
		State:    stateStruct,
	})
	if err != nil {
		log.Fatalf("CreateSession failed: %v", err)
	}
	fmt.Printf("Created session: %s\n", createResp.Session.Id)

	// 2. GetSession
	getResp, err := client.GetSession(ctx, &statestorepb.GetSessionRequest{
		Id:       createResp.Session.Id,
		TenantId: "tenant-henil",
	})
	if err != nil {
		log.Fatalf("GetSession failed: %v", err)
	}
	fmt.Printf("Fetched session: %+v\n", getResp.Session)

	// 3. ListSessionsByTenant
	listResp, err := client.ListSessionsByTenant(ctx, &statestorepb.ListSessionsByTenantRequest{
		TenantId: "tenant-henil",
	})
	if err != nil {
		log.Fatalf("ListSessionsByTenant failed: %v", err)
	}
	fmt.Printf("Tenant has %d sessions (via gRPC)\n", len(listResp.Sessions))
}
