#Multilayer

#Imagen base, se descarga pandas
FROM python:3.9 as py

RUN pip install pandas

#Se copian los archivos

FROM py
WORKDIR /app
COPY pipeline.py pipeline.py

ENTRYPOINT ["python", "pipeline.py"]