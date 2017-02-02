from extensions import sqlalchemy as db
from datetime import datetime
from database import Model

class Billing(Model):
    id = db.Column(db.Integer, primary_key=True)
    storage_cost = db.Column(db.Numeric)
    compute_cost = db.Column(db.Numeric)
    project = db.Column(db.Text)
    start_date = db.Column(db.DateTime)
    end_date = db.Column(db.DateTime)
    created_date = db.Column(db.DateTime, default=datetime.utcnow)
    closed_out = db.Column(db.Boolean, nullable=False, default=False)
    __table_args__ = (db.UniqueConstraint('project', 'start_date', name='unique_prj_start'),)

    def __init__(self, compute_cost, storage_cost, project, start_date, end_date, **kwargs):
        db.Model.__init__(self, compute_cost=compute_cost, storage_cost=storage_cost, project=project,
                          start_date=start_date,
                          end_date=end_date,
                        **kwargs)

    def __repr__(self):
        return "<Billing, Project: {} , Cost: {}, Time Range: {}-{}, Time created: {}".format(
                self.project, self.cost, str(self.start_date),
                str(self.end_date), str(self.created_date))

    def to_json(self):
        dict_representation = {}
        dict_representation["cost"] = str(self.cost)
        dict_representation["compute_cost"] = str(self.compute_cost)
        dict_representation["storage_cost"] = str(self.storage_cost)
        dict_representation["project"] = self.project
        dict_representation["start_date"] = datetime.strftime(self.start_date, format="%a %b %d %H:%M:%S %Z %Y")
        dict_representation["end_date"] = datetime.strftime(self.end_date, format="%a %b %d %H:%M:%S %Z %Y")
        return dict_representation

    def __close_out__(self):
        self.end_date = datetime.utcnow
        self.closed_out = True

    @property
    def cost(self):
        return self.compute_cost+self.storage_cost
