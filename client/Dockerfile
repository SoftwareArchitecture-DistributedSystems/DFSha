FROM python:3.11-slim
WORKDIR /app
# Las dependencias van primero para aprovechar la caché de capas
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared ./shared
COPY proto ./proto
COPY client ./client
# Los stubs se generan en la imagen; así no hace falta subirlos al repo
RUN python -m grpc_tools.protoc -I proto --python_out=proto/generated --grpc_python_out=proto/generated \
    proto/auth.proto proto/metadata.proto proto/data_node.proto
# proto/generated tiene que estar en el path porque los _pb2 se importan de forma absoluta
ENV PYTHONPATH=/app:/app/proto/generated PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "client/main.py"]
