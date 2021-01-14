FROM python:3.6

# Installing gcloud package (includes gsutil)
RUN curl https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz > /tmp/google-cloud-sdk.tar.gz
RUN mkdir -p /usr/local/gcloud \
  && tar -C /usr/local/gcloud -xvf /tmp/google-cloud-sdk.tar.gz \
  && /usr/local/gcloud/google-cloud-sdk/install.sh
ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin

COPY . /google-bucket-manifest

RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python

# cache so that poetry install will run if these files change
COPY poetry.lock pyproject.toml /opt/ctds/qabot/

# install qa-bot and dependencies via poetry
RUN source $HOME/.poetry/env \
    && poetry install --no-dev --no-interaction \
    && poetry show -v

WORKDIR /google-bucket-manifest
