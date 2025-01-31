FROM apache/airflow:2.10.4

COPY requirements.txt /requirements.txt

RUN pip install --upgrade pip


# RUN pip install --no-cache-dir numpy==1.23.5  # Install an older numpy version


RUN pip install --no-cache-dir -r /requirements.txt
