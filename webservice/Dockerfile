FROM nginx:alpine
#Update Alpine from 3.4 to 3.5
RUN sed -i -e 's/v3\.4/v3.5/g' /etc/apk/repositories
RUN apk update
RUN apk upgrade --available
RUN sync
RUN reboot

#Set the working directory
WORKDIR /app

#Install python and other things
RUN apk add --update python py2-pip openssl ca-certificates py-openssl wget
RUN pip install --upgrade pip
RUN apk add --update uwsgi-python py-psycopg2 postgresql
RUN apk add --update bash
RUN apk add --update --no-cache gcc g++ py-lxml
#Copy the requirements file and install the python packages
ADD ./requirements.txt /app/requirements.txt
RUN apk --update add --virtual build-dependencies libffi-dev openssl-dev python-dev  build-base\
  && apk --update add --virtual libxml2-dev libxslt1-dev\
  && pip install -r /app/requirements.txt \
  && apk del build-dependencies

#Make NGINX run on the foreground
RUN echo "daemon off;" >> /etc/nginx/nginx.conf

#Remove default config file
RUN rm /etc/nginx/conf.d/default.conf
#Copy the modified Nginx conf
COPY nginx.conf /etc/nginx/conf.d/
# Copy the base uWSGI ini file to enable default dynamic uwsgi process number
COPY uwsgi.ini /etc/uwsgi/

#Install Supervisord
RUN apk add -y supervisor \
&& rm -rf /var/lib/apt/lists/*

# Custom Supervisord config
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

#Add the app code
COPY . /app

#Assign env variable
ENV FLASK_APP /app/mapi.py

#Remove the current uwsgi.ini
RUN rm /app/uwsgi.ini
#Add the in app uwsgi
ADD ./uwsgi/uwsgi.ini app/

RUN echo $(ls /etc/nginx/)

#Install bash
RUN echo "Hello service! Installing bash"
#RUN apk add --update bash 

CMD ["/usr/bin/supervisord"]
