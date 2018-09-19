import unittest
import mock
import flask

from webservice import authorize_with_dashboard

app = flask.Flask(__name__)


class CounterFunc(object):
    """
    Objects that act as functions that return True and count how
    many times they are called
    """
    def __init__(self):
        self.times_called = 0
        self.__name__ = 'counter_func'

    def __call__(self, *args, **kwargs):
        self.times_called += 1
        return True


class ResultObject(object):
    """A fake requests.py result"""
    def __init__(self, status_code):
        self.status_code = status_code
        self.text = 'this is text'


class TestAuth(unittest.TestCase):

    def test_access_denied(self):
        # look here http://flask.pocoo.org/docs/1.0/api/#flask.Flask.test_request_context
        # for how to set the headers, etc
        with app.test_request_context():
            with mock.patch('requests.get', return_value=ResultObject(401)):
                counter_func = CounterFunc()
                decorated = authorize_with_dashboard(counter_func)
                _, code = decorated()
                assert counter_func.times_called == 0
                assert code == 401

    def test_server_error(self):
        # look here http://flask.pocoo.org/docs/1.0/api/#flask.Flask.test_request_context
        # for how to set the headers, etc
        with app.test_request_context():
            with mock.patch('requests.get', return_value=ResultObject(500)):
                counter_func = CounterFunc()
                decorated = authorize_with_dashboard(counter_func)
                _, code = decorated()
                assert counter_func.times_called == 0
                assert code == 500

    def test_access_granted(self):
        # look here http://flask.pocoo.org/docs/1.0/api/#flask.Flask.test_request_context
        # for how to set the headers, etc
        with app.test_request_context():
            with mock.patch('requests.get', return_value=ResultObject(204)):
                counter_func = CounterFunc()
                decorated = authorize_with_dashboard(counter_func)
                assert decorated()
                assert counter_func.times_called == 1
