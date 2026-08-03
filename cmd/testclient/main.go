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
	// insecure.NewCredentials() = no TLS, fine for localhost dev
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

	// -- Test UpdateSession via gRPC --
	newStateStruct, err := structpb.NewStruct(map[string]interface{}{
		"note":  "updated via gRPC client",
		"stage": 2,
	})
	if err != nil {
		log.Fatalf("failed to build struct: %v", err)
	}

	updateResp, err := client.UpdateSession(ctx, &statestorepb.UpdateSessionRequest{
		Id:       createResp.Session.Id,
		TenantId: "tenant-henil",
		State:    newStateStruct,
	})
	if err != nil {
		log.Fatalf("UpdateSession failed: %v", err)
	}
	fmt.Printf("UpdatedSession sucessfully: %+v", updateResp.Session)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel2()

	// -- Test GetSessionAsOf via gRPC --
	time.Sleep(11 * time.Second) // give a clean gap before checking history

	asOfResp, err := client.GetSessionAsOf(ctx2, &statestorepb.GetSessionAsOfRequest{
		Id:       createResp.Session.Id,
		TenantId: "tenant-henil",
		Interval: "10 seconds",
	})
	if err != nil {
		log.Fatalf("GetSessionAsOf failed: %v", err)
	}
	fmt.Printf("Session ~10s ago: %+v\n", asOfResp.Session)

	// --- Test bad interval format (should be rejected by our regex guard) ---
	_, err = client.GetSessionAsOf(ctx2, &statestorepb.GetSessionAsOfRequest{
		Id:       createResp.Session.Id,
		TenantId: "tenant-henil",
		Interval: "5; DROP TABLE agent_sessions;", // deliberately malicious-looking input
	})
	if err != nil {
		fmt.Println("Correctly rejected malicious interval:", err)
	}

	// --- Test DeleteSession via gRPC ---
	deleteResp, err := client.DeleteSession(ctx2, &statestorepb.DeleteSessionRequest{
		Id:       createResp.Session.Id,
		TenantId: "tenant-henil",
	})
	if err != nil {
		log.Fatalf("DeleteSession failed: %v", err)
	}
	fmt.Printf("Delete success: %v\n", deleteResp.Sucess)

	// Confirm it's gone
	_, err = client.GetSession(ctx2, &statestorepb.GetSessionRequest{
		Id:       createResp.Session.Id,
		TenantId: "tenant-henil",
	})
	if err != nil {
		fmt.Println("Correctly confirmed deletion via gRPC:", err)
	}

}
