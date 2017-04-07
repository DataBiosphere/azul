from datetime import datetime
#from active_alchemy import ActiveAlchemy
#db = ActiveAlchemy()
from flask_sqlalchemy import SQLAlchemy
db = SQLAlchemy()

class CRUDMixin(object):
    """Mixin that adds convenience methods for CRUD (create, read, update, delete) operations."""

    @classmethod
    def create(cls, **kwargs):
        """Create a new record and save it the database."""
        instance = cls(**kwargs)
        return instance.save()

    def update(self, commit=True, **kwargs):
        """Update specific fields of a record."""
        for attr, value in kwargs.items():
            setattr(self, attr, value)
        return commit and self.save() or self

    def save(self, commit=True):
        """Save the record."""
        db.session.add(self)
        if commit:
            db.session.commit()
        return self

    def delete(self, commit=True):
        """Remove the record from the database."""
        db.session.delete(self)
        return commit and db.session.commit()


class Model(CRUDMixin, db.Model):
    """Base model class that includes CRUD convenience methods."""

    __abstract__ = True


class Billing(Model):
    id = db.Column(db.Integer, primary_key=True)
    storage_cost = db.Column(db.Numeric, nullable=False, default=0)
    compute_cost = db.Column(db.Numeric, nullable=False, default=0)
    project = db.Column(db.Text)
    start_date = db.Column(db.DateTime)
    end_date = db.Column(db.DateTime)
    created_date = db.Column(db.DateTime, default=datetime.utcnow)
    closed_out = db.Column(db.Boolean, nullable=False, default=False)
    cost_by_analysis = db.Column(db.JSON)
    __table_args__ = (db.UniqueConstraint('project', 'start_date', name='unique_prj_start'),)

    def __init__(self, compute_cost, storage_cost, project, cost_by_analysis, start_date, end_date, **kwargs):
        db.Model.__init__(self, compute_cost=compute_cost, storage_cost=storage_cost, project=project,
                          start_date=start_date,
                          end_date=end_date,
                          cost_by_analysis=cost_by_analysis,
                        **kwargs)

    def __repr__(self):
        return "<Billing, Project: {} , Cost: {}, Time Range: {}-{}, Time created: {}".format(
                self.project, self.cost, str(self.start_date),
                str(self.end_date), str(self.created_date))

    def to_json(self):
        dict_representation = {}
        dict_representation["cost"] = str(round(self.cost,2))
        dict_representation["compute_cost"] = str(round(self.compute_cost, 2))
        dict_representation["storage_cost"] = str(round(self.storage_cost,2))
        dict_representation["project"] = self.project
        dict_representation["start_date"] = datetime.strftime(self.start_date, format="%a %b %d %H:%M:%S %Z %Y")
        dict_representation["end_date"] = datetime.strftime(self.end_date, format="%a %b %d %H:%M:%S %Z %Y")
        dict_representation["by_analysis"] = self.cost_by_analysis
        dict_representation["month_of"] = datetime.strftime(self.start_date, format="%B-%Y")
        return dict_representation

    def __close_out__(self):
        self.end_date = datetime.utcnow
        self.closed_out = True

    @property
    def cost(self):
        return self.compute_cost+self.storage_cost
