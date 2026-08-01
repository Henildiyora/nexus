package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/Henildiyora/nexus/internal/db"
	"github.com/Henildiyora/nexus/internal/pb/statestore"
	"github.com/Henildiyora/nexus/internal/server"
	"github.com/joho/godotenv"
	"google.golang.org/grpc"
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

	// Create table if it does not already exist
	if err := db.InitSchema(ctx, pool); err != nil {
		log.Fatalf("Schema init failed: %v\n", err)
	}
	fmt.Println("Schema ready.")

	// Build the gRPC server
	grpcServer := grpc.NewServer()

	stateStoreServer := server.NewStateStoreServer(pool)

	// This line registers implementation with the gRPC machinery —
	// it's how gRPC knows "when a CreateSession call comes in over the network,
	// route it to stateStoreServer.CreateSession"
	statestore.RegisterStateStoreServiceServer(grpcServer, stateStoreServer)

	// Start listening
	port := ":50051" // standard-ish default gRPC dev port, arbitrary otherwise
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", port, err)
	}

	fmt.Printf("gRPC server listening on %s\n", port)

	// Server() blocks forever, handling incoming RPCs until the process is killed
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}
