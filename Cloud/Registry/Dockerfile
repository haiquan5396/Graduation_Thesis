FROM python:3.5-slim

MAINTAINER HaiQuan "haiquan5396@gmail.com"

ENV BROKER_CLOUD localhost
ENV HOST_DB localhost
ENV MODE PULL

COPY ./requirements.txt /app/requirements.txt

WORKDIR /app

RUN pip3 install -r requirements.txt

COPY ./Registry.py /app

CMD python3 Registry.py ${BROKER_CLOUD} ${MODE} ${HOST_DB}

#docker build -t haiquan5396/registry:1.0 .
#docker run -it --name registry-test -e "BROKER_CLOUD=192.168.60.248" -e "HOST_DB=192.168.60.248" -e "MODE=PUSH" haiquan5396/registry:1.0