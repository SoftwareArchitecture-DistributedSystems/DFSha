FROM python:3.11-slim
WORKDIR /app
# Dependencias primero, código después
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared ./shared
COPY proto ./proto
COPY data-node ./data-node
RUN python -m grpc_tools.protoc -I proto --python_out=proto/generated --grpc_python_out=proto/generated \
    proto/auth.proto proto/metadata.proto proto/data_node.proto
ENV PYTHONPATH=/app:/app/proto/generated PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "data-node/main.py"]
