FROM python:3-alpine
RUN pip3 install pika

COPY common ./common
COPY business/query4/dt_detect .

ENTRYPOINT ["python3", "main.py"]
