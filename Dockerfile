FROM python:3.6

COPY . /google-bucket-manifest

WORKDIR /google-bucket-manifest

RUN pip install -r requirements.txt