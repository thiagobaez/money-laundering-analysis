FROM python:3-alpine
RUN pip3 install pika

COPY common ./common
COPY business/query3/split_date .

ENTRYPOINT ["python3", "main.py"]
