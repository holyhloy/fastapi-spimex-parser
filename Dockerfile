FROM python:3.11-alpine3.20

WORKDIR /usr/src/app

ARG DB_HOST=db
ARG REDIS_HOST=redis

ENV DB_HOST="${DB_HOST}"
ENV REDIS_HOST="${REDIS_HOST}"

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1


RUN apk update && apk add postgresql-dev gcc python3-dev musl-dev


RUN pip install --upgrade pip
COPY requirements/requirements.txt .
RUN pip install -r requirements.txt


COPY . .