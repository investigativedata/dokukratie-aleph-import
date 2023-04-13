FROM python:3-bullseye

RUN apt-get update && apt-get -y upgrade
RUN pip install -U pip setuptools
RUN pip install alephclient banal mmmeta nomenklatura normality awscli boto3
RUN pip install "servicelayer @ git+https://github.com/investigativedata/servicelayer.git@feature/archive-format"

COPY Makefile /app/Makefile
COPY import.py /app/import.py
WORKDIR /app

ENV AWS_REGION=eu-central-1
ENV AWS_DEFAULT_REGION=eu-central-1
ENV ARCHIVE_BUCKET=dokukratie
ENV STATE_BUCKET=mmmeta-state
ENV ARCHIVE_ENDPOINT_URL=https://s3.investigativedata.org

ENTRYPOINT ["make"]
