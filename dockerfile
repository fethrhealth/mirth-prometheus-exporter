FROM python:3.12-slim
LABEL MAINTAINER="clayton.feathers"

WORKDIR /app
RUN pip install --no-cache-dir prometheus_client requests
COPY mirth_exporter.py mirthConfig.json ./
EXPOSE 9001

CMD ["python", "-u", "./mirth_exporter.py"]
