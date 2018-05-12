FROM python:3.5-slim

MAINTAINER HaiQuan "haiquan5396@gmail.com"

ENV BROKER_CLOUD localhost
ENV MODE PULL
ENV TIME_COLLECT 5

COPY ./requirements.txt /app/requirements.txt

WORKDIR /app

RUN pip3 install -r requirements.txt

COPY ./Collector.py /app

CMD python3 Collector.py ${BROKER_CLOUD} ${MODE} ${TIME_COLLECT}

#docker build -t haiquan5396/collector:1.2 .
#docker run -it --name collector-test -e "BROKER_CLOUD=192.168.60.248" -e "MODE=PUSH" haiquan5396/collector:1.2