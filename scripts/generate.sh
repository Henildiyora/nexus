#!/bin/bash
protoc \
  --go_out=internal/pb --go_opt=paths=source_relative \
  --go-grpc_out=internal/pb --go-grpc_opt=paths=source_relative \
  --proto_path=proto \
  --proto_path=$(go env GOPATH)/pkg/mod/google.golang.org/protobuf@v1.36.11 \
  proto/statestore/statestore.proto