FROM python:3.10
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk-headless && \
    apt-get clean

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH

WORKDIR /exoplanets
COPY . .
RUN pip install -r requirements.txt
CMD [ "python", "main.py"]
EXPOSE 3000

# syntax=docker/dockerfile:1
