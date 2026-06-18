FROM python:3-slim

RUN pip3 install pika docker

COPY common ./common
COPY watchdog ./watchdog

ENTRYPOINT ["python3", "-m", "watchdog.main"]
