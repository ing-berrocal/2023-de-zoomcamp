#Multilayer

#Imagen base, se descarga pandas
FROM python:3.9 as py

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 fastparquet

#Se copian los archivos

FROM py
WORKDIR /app
COPY ingest_data_from_url.py ingest_data_from_url.py

ENTRYPOINT ["python", "ingest_data_from_url.py"]