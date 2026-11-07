FROM python:3.11-slim
WORKDIR /app
# Instalamos dependencias antes de copiar el código para no reinstalar en cada cambio
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared ./shared
COPY proto ./proto
COPY metadata-server ./metadata-server
RUN python -m grpc_tools.protoc -I proto --python_out=proto/generated --grpc_python_out=proto/generated \
    proto/auth.proto proto/metadata.proto proto/data_node.proto
ENV PYTHONPATH=/app:/app/proto/generated PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "metadata-server/main.py"]
