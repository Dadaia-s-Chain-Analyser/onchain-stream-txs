FROM python:3.8-slim-buster



WORKDIR /app

COPY ./requirements.txt /app

RUN pip install --upgrade pip && \
    pip install -r requirements.txt


COPY ./src /app

RUN ln -s /app/scripts/run_alles.sh /usr/local/bin/RUN_ALLES


ENTRYPOINT [ "sleep", "infinity" ]