FROM cimg/python:3.7

USER root

RUN adduser \
    --shell /usr/bin/bash \
    --no-create-home \
    --uid 1000 \
    --ingroup circleci \
    --gecos "" \
    --disabled-login \
    curation

RUN mkdir -p /home/circleci/project/curation/data_steward \
    /home/circleci/project/curation/tests \
    /home/circleci/project/curation/tools \
    && chown -R curation:circleci /home/circleci/project/curation

RUN cd / \
    && wget -q https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-353.0.0-linux-x86_64.tar.gz \
    && tar -xzf google-cloud-sdk-353.0.0-linux-x86_64.tar.gz \
    && rm google-cloud-sdk-353.0.0-linux-x86_64.tar.gz \
    && cd ./google-cloud-sdk \
    && ./install.sh --quiet \
    && cd .. && ./google-cloud-sdk/bin/gcloud components update --quiet

RUN echo "source /google-cloud-sdk/path.bash.inc" | tee -a /home/circleci/.profile >> /home/circleci/.bashrc
RUN echo "gcloud auth activate-service-account --key-file=\${GOOGLE_APPLICATION_CREDENTIALS}" | tee -a /home/circleci/.profile >> /home/circleci/.bashrc

USER circleci

VOLUME "/home/circleci/project/curation/data_steward"
VOLUME "/home/circleci/project/curation/tests"
VOLUME "/home/circleci/project/curation/tools"
