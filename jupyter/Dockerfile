FROM jupyter/pyspark-notebook:spark-3.3.0
USER root

COPY requirements.txt /home/jovyan/
RUN pip install -r /home/jovyan/requirements.txt

COPY jupyter_lab_config.json /home/jovyan/
