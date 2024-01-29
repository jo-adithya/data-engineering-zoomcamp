FROM python:3.11

WORKDIR /src

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

COPY ingest_data.py .

ENTRYPOINT [ "python", "ingest_data.py" ]

