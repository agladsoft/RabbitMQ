FROM python:3.8

ARG XL_IDP_PATH_DOCKER

ENV XL_IDP_PATH_DOCKER=${XL_IDP_PATH_DOCKER}

# Install Cron
RUN apt-get update && apt-get -y install cron

RUN chmod -R 777 $XL_IDP_PATH_DOCKER

COPY requirements.txt ./

COPY *.py ./

RUN pip install -r requirements.txt

COPY crontab /etc/cron.d/crontab
RUN chmod 0644 /etc/cron.d/crontab
RUN crontab /etc/cron.d/crontab

CMD ["/bin/sh", "-c", "printenv > /etc/environment && cron -f"]