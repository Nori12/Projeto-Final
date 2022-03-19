FROM python:3.10.2-bullseye

ENV STOCK_MARKET_DB_USER postgres
ENV STOCK_MARKET_DB_PASS 1234
ENV STOCK_MARKET_DB_PORT 5432
ENV STOCK_MARKET_DB_NAME StockMarket
ENV STOCK_MARKET_DB_HOST db

RUN apt update

RUN apt install wget
ENV DOCKERIZE_VERSION v0.5.0
RUN wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

RUN mkdir -p /home/app

COPY requirements.txt /home/app
COPY config /home/app/config
COPY logs /home/app/logs
COPY machine_learning /home/app/machine_learning
COPY src /home/app/src

WORKDIR /home/app

RUN pip3 install --upgrade pip

RUN pip3 install -r /home/app/requirements.txt

CMD dockerize -wait tcp://db:5432 -timeout 1m && python3 -Wignore src/main.py