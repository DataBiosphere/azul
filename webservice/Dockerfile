FROM nginx:1.10
# upgrade pip and install required python packages
RUN apt-get update
RUN apt-get install -y build-essential libpq-dev libssl-dev libffi-dev python-dev
RUN apt-get install -y python-pip
RUN pip install -U pip
RUN pip install uwsgi
RUN pip install --upgrade cffi
#WORKDIR /app
RUN apt-get install -y uwsgi-plugin-python
ADD ./requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt
#Make NGINX run on the foreground
RUN echo "daemon off;" >> /etc/nginx/nginx.conf

#Remove default config file
RUN rm /etc/nginx/conf.d/default.conf
#Copy the modified Nginx conf
COPY nginx.conf /etc/nginx/conf.d/
# Copy the base uWSGI ini file to enable default dynamic uwsgi process number
COPY uwsgi.ini /etc/uwsgi/

# Install Supervisord
RUN apt-get update && apt-get install -y supervisor \
&& rm -rf /var/lib/apt/lists/*
# Custom Supervisord config
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

ENV APACHE_PATH=""

# Add app code
COPY . /app
#Remove the current uwsgi.ini
RUN rm /app/uwsgi.ini
#Add the in app uwsgi
ADD ./uwsgi/uwsgi.ini app/
#Make the working directory /app
WORKDIR /app

CMD ["/usr/bin/supervisord"]
