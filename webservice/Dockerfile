FROM ubuntu:18.04
# upgrade pip and install required python packages
RUN apt-get update
RUN apt-get install -y build-essential libpq-dev libssl-dev libffi-dev python-dev libxml2-dev libxslt-dev python3-dev curl
RUN apt-get install -y python-pip python3-pip
RUN pip install -U pip
RUN pip3 install faker_schema
RUN pip install --upgrade cffi
ADD ./requirements.txt /app/requirements.txt
RUN pip install --ignore-installed -r /app/requirements.txt

# Add app code
COPY . /app
WORKDIR /app
CMD ["./go.sh"]
