FROM python:3.7
MAINTAINER salam1@woolies
RUN mkdir app 
WORKDIR app

ADD src .

RUN pip install -r requirements.txt

ENTRYPOINT ["python3"]