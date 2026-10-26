FROM python:3.12-slim
LABEL MAINTAINER="clayton.feathers"

WORKDIR /app
RUN pip install --no-cache-dir prometheus_client requests

# Runs as a fixed non-root uid rather than root: the exporter only ever makes
# outbound HTTPS calls to Mirth and serves its own /metrics, neither of which
# needs root, and a container compromise here should not land as root.
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin exporter
COPY mirth_exporter.py mirthConfig.json ./
USER exporter

EXPOSE 9001

# Prometheus's own scrape loop already proves the exporter is up; this is for
# the container orchestrator (compose, k8s) to know the process didn't wedge.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:9001/metrics', timeout=3)"

CMD ["python", "-u", "./mirth_exporter.py"]
