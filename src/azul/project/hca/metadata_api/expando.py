class Expando(dict):
    """
    Pass inital attributes to the constructor:

    >>> o = Expando(foo=42)
    >>> o.foo
    42

    Dynamically create new attributes:

    >>> o.bar = 'hi'
    >>> o.bar
    'hi'

    Expando is a dictionary:

    >>> isinstance(o,dict)
    True
    >>> o['foo']
    42

    Works great with JSON:

    >>> import json
    >>> s='{"foo":42}'
    >>> o = json.loads(s,object_hook=Expando)
    >>> o.foo
    42
    >>> o.bar = 'hi'
    >>> o.bar
    'hi'

    And since Expando is a dict, it serializes back to JSON just fine:

    >>> json.dumps(o, sort_keys=True)
    '{"bar": "hi", "foo": 42}'

    Attributes can be deleted, too:

    >>> o = Expando(foo=42)
    >>> o.foo
    42
    >>> del o.foo
    >>> o.foo
    Traceback (most recent call last):
    ...
    AttributeError: 'Expando' object has no attribute 'foo'
    >>> o['foo']
    Traceback (most recent call last):
    ...
    KeyError: 'foo'

    >>> del o.foo
    Traceback (most recent call last):
    ...
    AttributeError: foo

    And copied:

    >>> o = Expando(foo=42)
    >>> p = o.copy()
    >>> isinstance(p,Expando)
    True
    >>> o == p
    True
    >>> o is p
    False

    Same with MagicExpando ...

    >>> o = MagicExpando()
    >>> o.foo.bar = 42
    >>> p = o.copy()
    >>> isinstance(p,MagicExpando)
    True
    >>> o == p
    True
    >>> o is p
    False

    ... but the copy is shallow:

    >>> o.foo is p.foo
    True
    """

    def __init__( self, *args, **kwargs ):
        super( Expando, self ).__init__( *args, **kwargs )
        self.__slots__ = None
        self.__dict__ = self

    def copy(self):
        return type(self)(self)

# source: https://github.com/BD2KGenomics/bd2k-python-lib/blob/master/src/bd2k/util/expando.py
