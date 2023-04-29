ARG IMAGE_VERSION="3.9-slim"

# ============================================================
FROM python:${IMAGE_VERSION} as builder

USER root

COPY ./../apps/requirements.txt /tmp/src/requirements.txt
COPY ./../sa_tools /tmp/src/sa_tools


RUN python -m venv /opt/sentiment-analysis \
 && opt/sentiment-analysis/bin/pip install --no-cache-dir --upgrade pip setuptools wheel  \
 && opt/sentiment-analysis/bin/pip install --no-cache-dir -r /tmp/src/requirements.txt \
 && opt/sentiment-analysis/bin/pip install --no-cache-dir /tmp/src/sa_tools/ \
 ;


# ============================================================
FROM python:${IMAGE_VERSION}

RUN apt-get update \
 && apt-get install -y procps htop

# Copy tini and the pyhton package into /opt from the builder image
COPY --from=builder /opt/sentiment-analysis /opt/sentiment-analysis
COPY ./../apps /app/apps

ENV PATH="/opt/sentiment-analysis/bin:${PATH}"

WORKDIR /app/apps
