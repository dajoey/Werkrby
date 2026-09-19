FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app/ ./app/
COPY helm/ ./helm/
ENV HELM_DB=/data/helm.db HELM_ATTACH_DIR=/data/attachments HELM_PORT=3038
EXPOSE 3038
VOLUME /data
CMD ["python3", "-m", "uvicorn", "app.service:app", "--host", "0.0.0.0", "--port", "3038"]
