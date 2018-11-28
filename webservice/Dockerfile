FROM nginx:1.15
# upgrade pip and install required python packages
RUN apt-get update
# See https://github.com/debuerreotype/debuerreotype/issues/10
RUN mkdir /usr/share/man/man1
RUN mkdir /usr/share/man/man7
RUN apt-get install -y build-essential libpq-dev libssl-dev libffi-dev python-dev
RUN apt-get install -y python-pip postgresql
RUN pip install -U pip setuptools
RUN pip install uwsgi
RUN pip install --upgrade cffi
#WORKDIR /app
RUN apt-get install -y uwsgi-plugin-python
ADD ./requirements.txt /app/requirements.txt
RUN pip install --ignore-installed -r /app/requirements.txt
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
#Add log folder
RUN mkdir /app/log
#Add crontab file
ADD crontab /etc/cron.d/action-cron
RUN chmod 0644 /etc/cron.d/action-cron
RUN cron

# Java install: add PPA to sources list
RUN echo "deb http://ppa.launchpad.net/webupd8team/java/ubuntu xenial main" | tee /etc/apt/sources.list.d/webupd8team-java.list
RUN echo "deb-src http://ppa.launchpad.net/webupd8team/java/ubuntu xenial main" | tee -a /etc/apt/sources.list.d/webupd8team-java.list
RUN echo "oracle-java8-installer shared/accepted-oracle-license-v1-1 select true" | /usr/bin/debconf-set-selections

# Fetch GPG key
RUN export GNUPGHOME="$(mktemp -d)"
RUN gpg --batch --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys C2518248EEA14886
RUN gpg --export --armor C2518248EEA14886 | tee /etc/apt/trusted.gpg.d/docker.gpg.asc
RUN rm -rf "$GNUPGHOME"

RUN apt-get update
RUN apt-get install -y curl dnsutils oracle-java8-installer

# Update certificates
RUN echo "deb-src  http://deb.debian.org/debian  stretch main" | tee -a /etc/apt/sources.list
RUN apt-get update
RUN apt-get install -y ca-certificates

CMD ["/usr/bin/supervisord"]
