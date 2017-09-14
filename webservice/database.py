from flask_login import LoginManager
from flask_sqlalchemy import SQLAlchemy
import datetime
from flask_login import UserMixin

db = SQLAlchemy()
login_db = SQLAlchemy()
login_manager = LoginManager()

class User(login_db.Model, UserMixin):
    __tablename__ = "users"
    __bind_key__ = "login-db"
    id = login_db.Column(login_db.Integer, primary_key=True)
    email = login_db.Column(login_db.String(100), unique=True, nullable=False)
    name = login_db.Column(login_db.String(100), nullable=True)
    avatar = login_db.Column(login_db.String(200))
    access_token = login_db.Column(login_db.String(5000))
    redwood_token = login_db.Column(login_db.String(5000))
    tokens = login_db.Column(login_db.Text)
    created_at = login_db.Column(login_db.DateTime, default=datetime.datetime.utcnow())
