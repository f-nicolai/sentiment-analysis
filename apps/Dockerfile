FROM python:3.9-slim

RUN apt-get update \
 && apt-get install -y procps htop

COPY ./../apps /app/apps
COPY ./../sa_tools /app/sa_tools
COPY ./../api_secrets.py /app/api_secrets.py
WORKDIR /app


RUN pip install -r apps/requirements.txt
