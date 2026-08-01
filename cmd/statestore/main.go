package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/Henildiyora/nexus/internal/db"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

func main() {

	// Load the .env file
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error in loading .env: %v", err)
	}

	// Get the connection string from .env file
	connectionString := os.Getenv("DATABASE_URL")

	ctx := context.Background()

	// Connect to DB
	pool, err := db.ConnectDB(ctx, connectionString)
	if err != nil {
		log.Fatalf("Connection faild: %v", err)
	}
	defer pool.Close()

	// Test the connection pool with ping
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("Ping faild %v\n", err)
	}
	fmt.Println("Connected to CockroachDB pool successfully!")

	// // Create table if it does not already exist
	// if err := db.InitSchema(ctx, pool); err != nil {
	// 	log.Fatalf("Schema init failed: %v\n", err)
	// }
	// fmt.Println("Schema ready.")

	// // Build the gRPC server
	// grpcServer := grpc.NewServer()

	// stateStoreServer := server.NewStateStoreServer(pool)

	// // This line registers implementation with the gRPC machinery —
	// // it's how gRPC knows "when a CreateSession call comes in over the network,
	// // route it to stateStoreServer.CreateSession"
	// statestore.RegisterStateStoreServiceServer(grpcServer, stateStoreServer)

	// // Start listening
	// port := ":50051" // standard-ish default gRPC dev port, arbitrary otherwise
	// lis, err := net.Listen("tcp", port)
	// if err != nil {
	// 	log.Fatalf("failed to listen on %s: %v", port, err)
	// }

	// fmt.Printf("gRPC server listening on %s\n", port)

	// // Server() blocks forever, handling incoming RPCs until the process is killed
	// if err := grpcServer.Serve(lis); err != nil {
	// 	log.Fatalf("failed to serve: %v", err)
	// }

	// // Create table if it doesn't already exist
	// if err := db.InitSchema(ctx, pool); err != nil {
	// 	log.Fatalf("Schema init failed: %v\n", err)
	// }
	// fmt.Println("Schema ready.")

	// // Insert a sample session — imagine this is an agent's working memory
	// sampleState := map[string]interface{}{
	// 	"last_action": "searched_docs_new",
	// 	"step_count":  3,
	// }
	// newID, err := db.InsertSession(ctx, pool, "agent-001", "tenant-henil", sampleState)
	// if err != nil {
	// 	log.Fatalf("Insert failed: %v\n", err)
	// }
	// fmt.Printf("Inserted session with ID: %s\n", newID)

	// // Read it back to prove round-trip works
	// session, err := db.GetSession(ctx, pool, newID, "tenant-henil")
	// if err != nil {
	// 	log.Fatalf("Get failed: %v\n", err)
	// }
	// fmt.Printf("Fetched session: %+v\n", session)

	sessionID, err := uuid.Parse("72e75ca4-38b1-434e-b180-6fee7039fc0a")
	// if err != nil {
	// 	log.Fatalf("invalid uuid: %v\n", err)
	// }
	tenantID := "tenant-henil"
	// newState := map[string]interface{}{"note": "Updated via UpdateSession"}
	// if err := db.UpdateSession(ctx, pool, sessionID, tenantID, newState); err != nil {
	// 	log.Fatalf("UpdateSession failed: %v\n", err)
	// }
	// fmt.Println("Session updated successfully")

	// // Confirm the update actually took effect
	// updated, err := db.GetSession(ctx, pool, sessionID, tenantID)
	// if err != nil {
	// 	log.Fatalf("GetSession after update failed: %v\n", err)
	// }
	// fmt.Printf("Session after update: %+v\n", updated)

	// // Time travel
	// fmt.Println("Sleeping 6 seconds before checking history...")
	// time.Sleep(6 * time.Second)

	// past, err := db.GetSessionAsOf(ctx, pool, sessionID, tenantID, "5 seconds")
	// if err != nil {
	// 	log.Fatalf("GetSessionAsOf failed: %v\n", err)
	// }
	// fmt.Printf("Session state ~5s ago (should be OLD state, before this update): %+v\n", past)

	// Delete
	if err := db.DeleteSession(ctx, pool, sessionID, tenantID); err != nil {
		log.Fatalf("Delete session failed: %v/n", err)
	}

	_, err = db.GetSession(ctx, pool, sessionID, tenantID)
	if err != nil {
		fmt.Println("Correctly confirmed deletion — session no longer found:", err)
	}

}
