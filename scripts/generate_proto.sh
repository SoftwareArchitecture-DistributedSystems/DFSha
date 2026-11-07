#!/bin/bash
# Genera los stubs de gRPC en proto/generated. Hay que correrlo después de tocar un .proto.
cd "$(dirname "$0")/.."
python3 -m grpc_tools.protoc -I proto --python_out=proto/generated --grpc_python_out=proto/generated \
  proto/auth.proto proto/metadata.proto proto/data_node.proto
echo "stubs generados en proto/generated"
