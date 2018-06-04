FROM ubuntu:18.04
# upgrade pip and install required python packages
RUN apt-get update
RUN apt-get install -y build-essential libpq-dev libssl-dev libffi-dev python-dev
RUN apt-get install -y python-pip 
RUN pip install -U pip
RUN pip install --upgrade cffi
ADD ./requirements.txt /app/requirements.txt
RUN pip install --ignore-installed -r /app/requirements.txt

# Add app code
COPY . /app
WORKDIR /app
CMD ["./go.sh"]