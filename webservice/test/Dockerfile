FROM ubuntu:18.04
# upgrade pip and install required python packages
RUN apt-get update
RUN apt-get install -y build-essential libpq-dev libssl-dev libffi-dev libxml2-dev libxslt-dev python3-dev curl
RUN apt-get install -y python3-pip
RUN pip3 install -U pip
RUN pip3 install faker_schema
RUN pip3 install --upgrade cffi
ADD ./requirements.txt /app/requirements.txt
RUN pip install --ignore-installed -r /app/requirements.txt

# Add app code
COPY . /app
WORKDIR /app
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
#CMD ["sh", "-c", "tail -f /dev/null"]
CMD ["./go.sh"]
