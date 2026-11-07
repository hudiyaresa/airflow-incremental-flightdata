FROM apache/airflow:2.10.2

USER root
RUN apt-get update && apt-get install -y --no-install-recommends wget

COPY requirements.txt /requirements.txt
COPY start.sh /start.sh
RUN chmod +x /start.sh

USER airflow

RUN pip install --no-cache-dir -r /requirements.txt

ENTRYPOINT ["/bin/bash", "/start.sh"]
