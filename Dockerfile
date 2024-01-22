FROM python:3.7

COPY --from=openjdk:11 /usr/local/openjdk-11 /usr/local/openjdk-11

ENV JAVA_HOME /usr/local/openjdk-11

RUN update-alternatives --install /usr/bin/java java /usr/local/openjdk-11/bin/java 1
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV PYSPARK_MAJOR_PYTHON_VERSION=3

WORKDIR /app

COPY requirements.txt ./
RUN pip install -r requirements.txt
COPY /data /app/data
COPY . /app
CMD ["python3", "tft_analyzer/main.py", "run", "-j", "preprocess", "-i", "bronze.matches", "-o", "silver.matches"]