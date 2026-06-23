FROM python:3-slim

RUN pip3 install docker

COPY chaos_monkey ./chaos_monkey

ENTRYPOINT ["python3", "-m", "chaos_monkey.main"]
