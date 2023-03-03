# docker build -t exoplanets .
FROM python:3.10
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk-headless && \
    apt-get clean

# dependencies for pyspark
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH

#TODO: user docker secrets
ENV AWS_ACCESS_KEY_ID=AKIA5EYHU5Z77TPPYJ5F
ENV AWS_SECRET_ACCESS_KEY=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
ENV AWS_DEFAULT_REGION=us-east-1

WORKDIR /exoplanets
COPY . .
RUN pip install -r requirements.txt
CMD [ "python", "main.py"]
EXPOSE 3000

# syntax=docker/dockerfile:1
