from action import actionbp
from database import login_db, login_manager
from flask import Flask
from flask_migrate import Migrate
import logging.config
from models import db
import os
from webservice import webservicebp

# Setup logging
base_path = os.path.dirname(os.path.abspath(__file__))
logging.config.fileConfig('{}/config/logging.conf'.format(base_path))
logger = logging.getLogger("dashboardService")


class Config(object):
    """
    Configuration class for accessing database for login
    """
    SQLALCHEMY_DATABASE_URI = os.environ.get("DATABASE_URL")
    SQLALCHEMY_BINDS = {
        'login-db': 'postgresql://{}:{}@login-db/{}'.format(
            os.getenv("L_POSTGRES_USER"),
            os.getenv("L_POSTGRES_PASSWORD"),
            os.getenv("L_POSTGRES_DB"))
    }
    SECRET_KEY = os.environ.get("SECRET_KEY") or "somethingsecret"


# Setup the flask application
app = Flask(__name__)
app.config['DEBUG'] = True
app.config.from_object(Config)
# Initialize the database
db.init_app(app)
# Setup the login manager with the login database
login_db.init_app(app)
login_manager.init_app(app)
# Further configuration of the login manager
login_manager.login_view = "login"
login_manager.session_protection = "strong"
# Register the blueprints for the action service monitor, the webservice,
# and the billing
app.register_blueprint(actionbp)
app.register_blueprint(webservicebp)
# Apply any migrations
migrate = Migrate(app, db)
# Get the app context
app.app_context()

if __name__ == '__main__':
    app.run()
