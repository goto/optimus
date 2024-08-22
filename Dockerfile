FROM alpine:3.13
ARG USER=optimus

COPY optimus /usr/bin/optimus
WORKDIR /app

COPY .kube-config /app/.kube-config

RUN adduser -D $USER 
RUN chown -R $USER:$USER /app

RUN apk add --no-cache tzdata curl

# Install dependencies: curl, bash, and python3 for gsutil
RUN apk --no-cache add \
    bash \
    python3 \
    py3-crcmod \
    gnupg \
    ca-certificates

# Install pip for Python
RUN python3 -m ensurepip && pip3 install --no-cache --upgrade pip

# Install Google Cloud SDK and gsutil
RUN echo "https://packages.cloud.google.com/apt/doc/apt-key.gpg" && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --import && \
    curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-431.0.0-linux-x86_64.tar.gz && \
    tar -xzvf google-cloud-sdk-431.0.0-linux-x86_64.tar.gz && \
    rm google-cloud-sdk-431.0.0-linux-x86_64.tar.gz && \
    ./google-cloud-sdk/install.sh --quiet

# Set environment variable for gcloud
ENV PATH=$PATH:/google-cloud-sdk/bin


# use this part on airflow task to fetch and compile assets by optimus client
COPY ./entrypoint_init_container.sh /opt/entrypoint_init_container.sh
COPY ./exit.sh /opt/exit.sh
RUN chmod +x /opt/entrypoint_init_container.sh
RUN chmod +x /opt/exit.sh

USER $USER

EXPOSE 8080
CMD ["optimus"]