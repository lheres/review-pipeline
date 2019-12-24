FROM puckel/docker-airflow:1.10.6
RUN python3 -m pip install --force-reinstall pip --user

WORKDIR /usr/local/airflow/
COPY requirements.txt .
RUN pip3 install --user -r requirements.txt

