# FROM apache/airflow:2.1.0-python3.8
FROM puckel/docker-airflow

# Copy boto file to docker image
COPY .boto /home/airflow/.boto

COPY ./dags /usr/local/airflow/dags

RUN apt-get update
RUN apt-get install vim
RUN apt install libnss
RUN apt install libnss3-dev libgdk-pixbuf2.0-dev libgtk-3-dev libxss-dev libgbm-dev
RUN npm install
RUN npm install -g typescript
RUN npm install -g ts-node
RUN npm i puppeteer
RUN node /usr/local/airflow/dags/resources/shopee_crawler/node_modules/puppeteer/install.js

# install requirements
COPY ./requirements.txt /usr/local/airflow/requirements.txt
RUN pip install --quiet --force -r /usr/local/airflow/requirements.txt