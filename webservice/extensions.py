from flask_sqlalchemy import SQLAlchemy
from flask_apscheduler import APScheduler
from flask_migrate import Migrate
from flask_elasticsearch import FlaskElasticsearch
sqlalchemy = SQLAlchemy()
scheduler = APScheduler()
migrate = Migrate()
elasticsearch = FlaskElasticsearch()
