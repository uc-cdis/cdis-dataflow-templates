FROM python:3.6

# Installing gcloud package (includes gsutil)
RUN curl https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz > /tmp/google-cloud-sdk.tar.gz
RUN mkdir -p /usr/local/gcloud \
  && tar -C /usr/local/gcloud -xvf /tmp/google-cloud-sdk.tar.gz \
  && /usr/local/gcloud/google-cloud-sdk/install.sh
ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin

RUN git clone https://github.com/vim/vim.git && cd vim/src && make && make install

COPY . /google-bucket-manifest

WORKDIR /google-bucket-manifest

RUN pip install -r requirements.txt