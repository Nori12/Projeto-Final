FROM python:3.10.2-bullseye

ENV STOCK_MARKET_DB_USER postgres
ENV STOCK_MARKET_DB_PASS 1234
ENV STOCK_MARKET_DB_PORT 5432
ENV STOCK_MARKET_DB_NAME StockMarket
ENV STOCK_MARKET_DB_HOST 172.17.0.2

RUN apt update
RUN apt install git

RUN mkdir -p /home/app

COPY requirements.txt /home/app
COPY config /home/app/config
COPY logs /home/app/logs
COPY machine_learning /home/app/machine_learning
COPY src /home/app/src

WORKDIR /home/app

RUN pip3 install --upgrade pip

RUN pip3 install -r /home/app/requirements.txt

CMD ["python3", "-Wignore", "src/main.py"]