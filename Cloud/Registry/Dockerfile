FROM python:3.5-slim

MAINTAINER HaiQuan "haiquan5396@gmail.com"

ENV BROKER_CLOUD localhost
ENV HOST_MYSQL localhost
ENV MODE PULL
ENV TIME_INACTIVE_PLATFORM 60
ENV TIME_UPDATE_CONF  2
ENV TIME_CHECK_PLATFORM_ACTIVE  2

COPY ./requirements.txt /app/requirements.txt

WORKDIR /app

RUN pip3 install -r requirements.txt

COPY ./Registry.py /app

CMD python3 Registry.py ${BROKER_CLOUD} ${MODE} ${HOST_MYSQL} ${TIME_INACTIVE_PLATFORM} ${TIME_UPDATE_CONF} ${TIME_CHECK_PLATFORM_ACTIVE}

#docker build -t haiquan5396/registry:1.2 .
#docker run -it --name registry-test -e "BROKER_CLOUD=192.168.60.248" -e "HOST_MYSQL=192.168.60.248" -e "MODE=PUSH" haiquan5396/registry:1.2
