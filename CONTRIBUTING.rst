############
Contributing
############

This document contains guidelines that every contributor to this project should
follow. We call them guidelines as opposed to rules because, well, life
happens. If a contributor disregards a guideline, they should have a good
reason for that and should be considered accountable for the consequences.

.. sectnum::
    :depth: 2
    :suffix: .

.. contents::


Code Style
==========

PEP
-----

* For Python we use PEP8 with E722 disabled (type too general in except clause)
  and the maximum line length set to 120 characters.

Line length
-----------

* For prose (documentation, comments) wrap lines at the word boundary closest to
  or at, but not beyond, column 79. The first column is column 0.

* For code, we keep the trimmed line length under 81. A trimmed line is a line
  in the source with leading and trailing whitespace removed. This means a line
  may be indented by 40 characters and contain 80 characters after that. This
  rule is designed to keep code readable without forcing excessive wrapping for
  more deeply nested control flow constructs.

String literals
---------------

* We prefer single quoted string literals. We used to use double quotes in JSON
  literals but that convention is now deprecated and all new string literals are
  single quoted except as noted below.

* We don't escape a quote character within string literals. When a string
  literal contains a single quote, that literal is delimited by double quotes
  (and vice versa). https://www.python.org/dev/peps/pep-0008/#string-quotes

Line wrapping and indentation
-----------------------------

* We prefer aligned indent for wrapped constructs except for collection literals
  collection comprehensions and generator expressions::

    self.call_me(positional,
                 x=1,
                 y=2)

    foo = {
        'foo': False,
        'a': [1, 2, 3]
    }

    bar = {
        k.upper(): v.lower
        for k,v in d.items()
        if k.startswith('x')
    }

* Small literal collections may be kept on one line up to the maximum line
  length. A small collection is one that has no more than 9 elements, all of
  which either primitive values or other small collections.

* We wrap all elements or none. Instead of ::

    self.call_me(foo, bar,
                 x=1, y=2)

  we use ::

    self.call_me(foo,
                 bar,
                 x=1,
                 y=2)

  The one exception to this rule are logging method invocations and calls to
  reject() and require()::

    logger.info('Waiting up to %s seconds for %s queues to %s ...',
                timeout, len(queues), 'empty' if empty else 'not be empty')

    reject(spline not in reticulated_splines,
           'Unreticulated splines cause discombobulation.')

  Only if the second and subsequent arguments won't fit on one line, do we
  wrap all arguments, one line per argument.

Trailing commas
---------------

* We don't use trailing commas in enumerations to optimize diffs yet. [#]_

.. [#] Note: If we were to adopt trailing commas, we would also have to
       abandon our preference of aligned indent.

Backslashes
-----------

* We avoid the use of backslash for continuing statements beyond one line.
  Instead, we exploit the fact that Python can infer continuation if they
  occur in balanced constructs like brackets or parentheses. If necessary we
  introduce a pair of parentheses around the wrapping expression.

  With some keywords it is impossible to add semantically insignificant
  parentheses. For example, ``assert foo, 'bad'`` is not equivalent to ``assert
  (foo, 'bad')``. In these exceptional situations it is permissible to use
  backslash for line continuation.

String interpolation
--------------------

* When interpolating strings into human-readable strings like log or exception
  messages, we use the ``!r`` format modifier (as in ``f'foo is {foo!r}'``) or
  ``%r`` in log messages. This automatically adds quotes around the interpolated
  string.

* Except for log messages (see below), we don't use the ``%`` operator or the
  ``str.format()`` method. We use ``f''`` strings or string concatenation. When
  choosing between the latter two, we use the one that yields the shortest
  expression. When both alternatives yield an expression of equal lengths, we
  prefer string concatenation::
  
    f'{a}{b}'  # Simple concatenation of variables
    a + b      # tends to be longer with f'' strings
    
    a + str(b) # {} calls str implicitly so f'' strings win
    f'{a}{b}'  # if any of the variables is not a string

    a + ' ' + b + '.tsv'  # When multiple literal strings are involved
    f'{a} {b}.tsv'        # f'' strings usually yield shorter expressions

String concatenation
--------------------

* We use ``str.join()`` when joining more than three elements with the same
  character or when the elements are already in an iterable form::
  
    f'{a},{b},{c},{d}'     # while this is shorter
    ','.join((a, b, c, d)) # this is more readable
  
    f'{a[0],a[1]}  # this is noisy and tedious
    ','.join(a)    # this is not
 
* We use `EAFP`_ as a principle.

.. _EAFP: https://stackoverflow.com/questions/11360858/what-is-the-eafp-principle-in-python

Variable names
--------------

* We don't use all upper case (all-caps) names for pseudo constants::

    CONSTANT_FOO = 'value_bar'  # bad
    constant_foo = 'value_bar'  # better

* The names of type variables are not necessarily limited to one character but
  we do use all-caps for them. In particular, names of bounded type variables
  should be more than a single character long, for example::

    SOURCE_REF = TypeVar('SOURCE_REF', bound='SourceRef')

* To name variables referencing a mapping like ``dict``, ``frozendict`` or
  ``Counter`` we prefer the ``values_by_key`` or ``key_to_value`` convention.

* The smaller the scope, the shorter the variable names we use. In ::

    def reticulate_splines(splines_to_reticulate):
        spline_reticulator = SplineReticulator()
        reticulated_splines = spline_reticulator.reticulate(splines_to_reticulate)
        return reticulated_splines

  the ``spline`` aspect is implied by the context provided by the method name
  so it can be omitted in the body::

    def reticulate_splines(splines):
        reticulator = SplineReticulator()
        splines = reticulator.reticulate(splines)
        return splines

  You catch my drift. Also note the reassignment.

* For tiny scopes like comprehensions, we even use single letter variable names
  if it's clear from the context what they mean::

    {k: str(v) for k, v in numeric_splines.items()}
    [ i * reticulate(s) in enumerate(numeric_splines.values())

  We prefer ``k`` and ``v`` for mapping keys and values, and ``i`` for counters.

Logging
-------

* Loggers are instantiated in every module that needs to log

* Loggers are always instantiated as follows::

    log = logging.getLogger(__name__) # is preferred for new code
    logger = logging.getLogger(__name__) # this is only OK in legacy code
  
* At program entry points we use the appropriate configuration method from
  ``azul.logging``. Program entry points are
  
  - in scripts::

      if __name__ == '__main__':
          configure_script_logging(log)

  - in test modules::

      def setUpModule():
          configure_test_logging(log)

  - in ``app.py``::

      log = logging.getLogger(__name__)
      app = AzulChaliceApp(app_name=config.indexer_name)
      configure_app_logging(app, log)

* We don't use ``f''`` strings or string concatenation when interpolating
  dynamic values into log messages::

    log.info(f'Foo is {bar}')  # don't do this
    log.info('Foo is %s', bar)  # do this
  
* Computationally expensive interpolations should be guarded::

    if log.isEnabledFor(logging.DEBUG):
        log.debug('Foo is %s', json.dump(giant, indent=4)

* Log and exception messages should not end in a period unless the message
  contains multiple sentences. If it does, all sentences in the message should
  end in a period, including a period at the end of the string.

Imports
-------

* We prefer absolute imports.

* We sort imports first by category, then lexicographically by module name and
  then by imported symbol. The categories are

  1. Import of modules in the Python runtime
    
  2. Imports of modules in external dependencies of the project
    
  3. Imports of modules in the project

* To minimize diffs and reduce the potential for merge conflicts, only one
  symbol may imported per line. When using ``from`` imports, all imported
  symbols must be wrapped in parentheses, indented, and the last symbol must
  have a trailing comma. Note that this applies even if only *one* symbol is
  imported. Thus, assuming that ``foo`` and ``bar`` are from the same category,
  ::

    import foo
    from foo import (
        glue,
        shoe,
    )
    import bar
    from bar import (
        far,
    )

  Is the *only* correct sequence of import statements for these symbols.

* We carefully selected the ordering criteria to match those implemented by
  PyCharm. PyCharm's *Optimize Imports* feature should be the preferred method
  of resolving import statement ordering violations, as the line numbers
  reported by our flake8 plugin are not always optimal in illuminating the
  nature of the violations.

* The one violation *not* addressable via PyCharm is our requirement that
  single-symbol ``from`` imports be wrapped the same as multi-symbol ones.
  Currently, this must be corrected manually. Vim users may find the following
  macro convenient for this purpose:
  ::

    ^3Wi(<ENTER><ESCAPE>A,<ENTER>)

Comments
--------

* We don't use inline comments to explain what should be obvious to software
  engineers familiar with the project. To help new contributors become
  familiar, we document the project architecture and algorithms separately from
  the Python source code in a ``docs`` subdirectory of the project root. 

* When there is the need to explain in the source, we focus on the Why rather
  than the How.


Inline Documentation
--------------------

* We use docstrings to document the purpose of an artifact (module, class,
  function or method), and its contract between with client code using it. We
  don't specify implementation details in docstrings.

* We put the triple quotes that delimit docstrings on separate lines::

    def foo():
        """
        Does nothing.
        """
        pass
        
  This visually separates function signature, docstring and function body from
  each other.

* Any method or function whose purpose isn't obvious by examining its signature
  (name, argument names and type hints, return type hint) should be documented
  in a docstring.

* Every external-facing API must have a docstring. An external-facing API is a
  class, function, method, attribute or constant that's exposed via Chalice
  or—if we ever were to release a library for use by other developers—exposed
  in that library.
  

Code duplication
----------------

* We avoid duplication of code and continually refactor it with the goals of
  reducing entropy while increasing consistency and reuse.

Consistency and precedent
-------------------------

* We try to follow existing precedent: we emulate what people did before us
  unless there is a good reason not to do so. Taste and preference are not good
  reasons because those differ from person to person.

  If resolving an issue requires touching a section of code that consistently
  violates the guidelines laid out herein, we either

  a) follow the precedent and introduce another violation or

  b) change the entire section to be compliant with the guidelines.

  Both are acceptable. We weigh the cost of extending the scope of our current
  work against the impact of perpetuating a problem. If we decide to make the
  section compliant, we do so in a separate commit. That commit should not
  introduce semantic changes and it should precede the commit that resolves the
  issue.

Ordering artifacts in the source
--------------------------------

* We generally use top-down ordering of artifacts within a module or script.
  Helper and utility artifacts succeed the code that use them. Bottom-up
  ordering—which has the elementary building blocks occur first—makes it harder
  to determine the purpose and intent of a module at a glance.

Disabling sections of code
--------------------------

* To temporarily disable a section of code, we embed it in a conditional
  statement with an test that always evaluates to false (``if False:`` in
  Python) instead of commenting that section out. We do this to keep the code
  subject to refactorings and code inspection tools.

Control flow
------------

* We avoid using bail-out statements like ``continue``, ``return`` and
  ``break`` unless not using them would require duplicating code, increase the
  complexity of the control flow or cause an excessive degree of nesting.
  
  Examples from the limited set of cases in which bail-outs are preferred::

    while True:
        <do something>
        if <condition>:
            break
        <do something else>

  can be unrolled into

  ::

    <do something>
    while not <condition>:
        <do something else>
        <do something>
        
  but that requires duplicating the ``<do something>`` section. In this case
  the use of ``break`` is preferred.
  
  Similarly,
  
  ::
  
    while <condition0>:
        if not <condition1>:
            <do something1>
            if not <condition2>:
                <do something2>
                if not <condition3>:
                    <do something3>
                    
  can be rewritten as
  
  ::

    while <condition0>:
        if <condition1>:
            continue
        <do something1>
        if <condition2>:
            continue
        <do something2>
        if <condition3>:
            continue
        <do something3>
        
  This eliminates the nesting which may in turn require fewer wrapped lines in
  the ``<do something …>`` sections, leading to increased readability.
  
* We add ``else`` for clarity even if its use isn't semantically required::
  
    if <condition>
        <do something1>
        return X
    <do something2>
    return Y
    
  should be written as
  
  ::
  
    if <condition>
        <do something1>       
        return X
    else:
        <do something2>
        return Y
  
  The latter clearly expresses the symmetry between and the equality of the two
  branches. It also reduces the possibility of introducing a defect if the code
  is modified to eliminate the ``return`` statements::
  
    if <condition>
        <do something1>
    <do something2>
    
  is broken, while the modified version with else remains intact::
  
    if <condition>
        <do something1>       
    else:
        <do something2>

Static methods
--------------

* We always use ``@classmethod`` instead of @staticmethod, even if the first
  argument (cls) of such a method is not used by its body. Whether cls is used
  is often incidental and an implementation detail. We don't want to repeatedly
  switch from ``@staticmethod`` to ``@classmethod`` and back if that
  implementation detail changes. We simply declare all methods that should be
  invoked through the class (as opposed to through an instance of that class) as
  ``@classmethod`` and call it a day.

  The same consideration goes for instance methods and ``self``: some use it,
  some don't. The ones that don't shouldn't suddenly be considered static
  methods. The distinction between instance and class methods is driven by
  higher order concerns than the one about whether a method's body currently
  references self or not.

Error messages
--------------

* We avoid the use of f-strings when composing error messages for exceptions and
  for use with ``require()`` or ``reject()``. If an error message is included,
  it should be short, descriptive of the error encountered, and optionally
  followed by the relevant value(s) involved::

    raise KeyError(key)

    raise ValueError('Unknown file type', file_type)

* Requirement errors should always have a message, since they are intended for
  clients/users::

    require(delay >= 0, 'Delay value must be non-negative')

    require(url.scheme == 'https', "Unexpected URL scheme (should be 'https')", url.scheme)

    reject(entity_id is None, 'Must supply an entity ID')

    reject(file_path.endswith('/'), 'Path must not end in slash', path)

* Assertions are usually self-explanatory. Error messages should only be
  included when they are not::

    assert not debug

    assert isinstance(x, list), type(x)

    assert x == y, ('Misreticulated splines', x, y)

Catching exceptions
-------------------

* When catching expected exceptions, especially for `EAFP`_, we minimize the
  body of the try block::

    d = make_my_dict()
    try:
        x = d['x']
    except:
        <do stuff without x>
    else:
        <do stuff with x>

  This is not a mere cosmetic convention, it affects program correctness. If the
  call to ``make_my_dict`` were done inside the ``try`` block, a KeyError raised
  by it would be conflated with the one raised by d['x']. The latter is
  expected, the former usually constitutes a bug.

Raising exceptions
------------------

* When raising an exception without arguments, we prefer raising the class
  instead of raising an instance constructed without arguments::

    raise RuntimeError()  # bad
    raise RuntimeError

Type hints
----------

* We use type hints both to document intent and to facilitate type checking by
  an IDE or other tooling.
  
* When defining type hints for a function or method, we do so for all its
  parameters and the return value.
  
* We prefer the generic types from ``typing`` over non-generic ones from the
  ``collections`` module e.g., ``MutableMapping[K,V]`` or ``Dict[K,V]`` over
  ``dict``.

* For method/function *arguments* we prefer the least specific type
  possible e.g., ``Mapping`` over ``MutableMapping`` or ``Sequence`` over
  ``List``. For example, we don't use ``Dict`` for an argument unless it is
  actually modified by the function/method. When the choice is between ``Dict``
  or ``MutableMapping`` we use ``Dict`` for arguments even though ``Dict`` is
  actually more restrictive. The reason is that there doesn't seem to be any
  class that implements ``MutableMapping`` while not also being a subclass of
  ``Dict``. The longer-named ``MutableMapping`` does not actually result in more
  options for the caller.

* For method and function return values we specify the type that we anticipate
  to be useful to the caller without being overly specific. For example, we
  prefer ``Dict`` for the return type because ``Mapping`` would prevent the
  caller from modifying the returned dictionary, something that's typically not
  desirable. If we do want to prevent modification we would return a
  ``frozendict`` or equivalent and declare the return value as ``Mapping``.

* Owing to the prominence of JSON in the project we annotate variables
  containing deserialized JSON as such, using the ``JSON`` and ``MutableJSON``
  types from ``azul.typing``. Note that due to the lack of recursive types in
  PEP-484, ``JSON`` unrolls the recursion only three levels deep. This means
  that with ``x: JSON`` the expression ``x['a']['b']['c']`` would be of type
  ``JSON`` while ``x['a']['b']['c']['d']`` would be of type ``Any``.


Method and function arguments
-----------------------------

* Arguments declared as a keyword must be passed as keyword arguments at all
  call sites.

* For call sites with more than three passed arguments, all arguments should be
  passed as keywords, even positional arguments, if one of the arguments is
  passed as a keyword.

* At call sites that pass a literal expression to a function or method, consider
  passing the argument as a keyword. Instead of ::

    foo(x, {})
    bar(True)

  use ::

    foo(filters={})
    bar(delete=True)

  while leaving ::

    add(1, 2)
    setDelete(True)

  as is.

* We prefer enforcing the use of keyword arguments using keyword-only arguments
  as defined in `PEP-3102`_.


.. _PEP-3102: https://www.python.org/dev/peps/pep-3102/


Testing
=======

Coverage of new code
--------------------

* All new code should be covered by unit tests.

Coverage of legacy code
-----------------------

* Legacy code for which tests were never written should be covered when it is
  modified.

Subtests
--------

* Combinatorial tests (tests that exercise a number of combinations of inputs)
  should make use of ``unittest.TestCase.subTest()`` so a single failing
  combination doesn't prevent other combinations form being exercised.

* Sub-tests may makes sense even when there isn't a large number of
  combinations. Consider two independent tests that share an expensive fixture.
  Instead of isolating the two tests in separate ``TestCase`` whose
  ``setUpClass`` method sets up the expensive fixture, one might write a single
  test method as follows::

    def test_a_b(self):
        self.set_fixture_up()
        try:
            with self.subTest('a'):
                ...
            with self.subTest('b'):
                ...
        finally:
            self.tear_fixture_down()

  This can only be done if ``a`` and ``b`` are independent. Ask yourself:
  does testing ``b`` make sense even after ``a`` fails? Can I safely reorder
  ``a`` and ``b`` without affecting the result? If the answer is "no" to either
  question, you have to remove the ``self.subText()`` invocations.

* We don't use sub-tests for the sole purpose of marking different sections of
  test code.

Doctests
--------

* Code that doesn't require elaborate or expensive fixtures should use doctests
  if that adds clarity to the documentation or helps with expressing intent.
  Modules containing doctests must be registered in the ``test_doctests.py``
  script.

Integration tests
-----------------

* Code that can only be tested in a real deployment should be covered by an
  integration test.


Version Control
===============

Branches
--------

* Feature branches are merged into ``develop``. If a hotfix is made to a
  deployment branch other than ``develop``, that branch is also back-ported and
  merged into ``develop`` so that the hotfix eventually propagates to all
  deployments.

* During a promotion, the branch for a lower deployment (say, ``integration``)
  is merged into the branch for the next higher deployment.

* We commit independent changes separately. If two changes could be applied in
  either order, they should occur in separate commits. Two changes A and B of
  which B depends on A may still be committed separately if B represents an
  extension of A that we might want to revert while leaving A in place.

Commits
-------

* We separate semantically neutral changes from those that alter semantics by
  committing them separately, even if that would violate the previous rule. The
  most prominent example of a semantically neutral change is a refactoring. We
  also push every semantically neutral commit separately such that the build
  status checks on Github and Gitlab prove the commit's semantic neutrality.

* In theory, every individual commit should pass unit and integration tests. In
  practice, on PR branches with long histories not intended to be squashed, not
  every commit is built in CI. This is acceptable. [#]_

.. [#] Note: I am not a fan this rule but the desire to maintain a linear
       history by rebasing PR branches as opposed to merging them requires this
       loophole. When pushing a rebased PR branch, we'd have to build every
       commit on that branch individually. Exploitation of this loophole can be
       avoided by creating narrowly focused PRs with only one logical change
       and few commits, ideally only one. We consider the creation of PRs with 
       longer histories to be a privilege of the lead.

Split commits
-------------

* A split commit is a set of commits that represent a single logical change that
  had to be committed separately up for technical reasons, to fairly capture
  multiple authors' contributions, for example, or to avoid bloated diffs (see
  below). We refer to the set of commits as the *split commit* and the members
  of the set as the *part commit*.

* The title of a part commit always carries the M/N tag (see `Commit titles`_),
  where N is the number of parts while M is the ordinal of the part, reflecting
  the topological order order of the parts. Splitting a change that
  "reticulates splines" into two parts yields two commits having the titles

  - ``[1/2] Reticulate them splines for good measure (#123)`` and
  - ``[2/2] Reticulate them splines for good measure (#123)``

  respectively.

* The parts must be consecutive, except for split commits made to retain
  authorship. The parts of a commit that was split to retain authorship can have
  other commits in between the parts if there is pressing reason to do so.

* The body of the commit messages for each part should have prose to distinguish
  the parts, except for split commits made to retain authorship, where the
  distinction is obvious: each part reflects the author's contribution.

Bloated diffs
-------------

* We avoid bloated diffs. A bloated diff has semantic changes on top of large
  hunks of deletions that resemble additions somewhere else in the diff. We
  especially avoid insidiously bloated diffs where the semantic change occurs
  *within* one of those large hunks of deletions or additions. Bloated diffs
  distort authorship and are hard to review.

  * We avoid moving large amounts of code around via Cut & Paste unless there is
    a technical reason to do so. If there is, we commit the code change that
    moves the code as part 1/2 of a split commit, then commit the changes that
    maintain referential integrity as part 2/2. Any additional changes to the
    moved code are committed as a normal commit.

  * When splitting a file into multiple files, we identify the largest part
    and move the file so that its new name reflects the largest part. We commit
    that change as part 1/3 of a split commit to trigger Git's heuristic for
    detecting file renames. This maximizes the amount of authorship that is
    maintained. We then move the remaining parts into their respective files
    using the method in the previous bullet using 2/3 for moving the code and
    3/3 for maintaining referential integrity. It's acceptable for the 1/3
    commit to include any changes maintaining referential integrity during the
    file rename because those occur in different files and therefore don't risk
    tripping up the heuristic.

Commit titles
-------------

* If a commit resolves (or contributes to the resolution of) an issue, we
  mention that issue at the end of the commit title::

    Reticulate them splines for good measure (#123)

  Note that we don't use Github resolution keywords like "fixes" or "resolves".
  Any mention of those preceding an issue reference in a title would
  automatically close the issue as soon as the commit appears on the default
  branch. This is undesirable as we want to continue to track issues in
  ZenHub's *Merged* and *Done* pipelines even after the commit appears on the
  ``develop`` branch.

* We value `expressive and concise commit message titles`_ and try to adhere to
  Github's limit of 72 characters for the length of a commit message title.
  Beyond 72 characters, Github truncates the title at 69 characters and adds
  three dots (ellipsis) which is undesirable. Titles with lots of wide
  characters like ``W`` may still wrap (as opposed to being truncated) but
  that's improbable and therefore acceptable.

* We don't use a period at the end of commit titles.

* We use `sentence case`_ for commit titles.

.. _expressive and concise commit message titles: https://chris.beams.io/posts/git-commit/

.. _sentence case: https://utica.libguides.com/c.php?g=291672&p=1943001

* When reverting a commit, be it literally or "in spirit", we refer to the
  commit ID of the reverted commit in the body of the message of the reverting
  commit. The reverting commit message title should also include a reference
  to the issue whose resolution includes the reverted commit. For literal
  reverts the commit message should be `Revert "{title of reverted commit}"`
  Most Git tooling does this automatically. For example (a literal revert,
  done with SmartGit)::

    f733e71 Revert "Reticulate them splines (#123)"

            This reverts commit bb7a87bed2c0a25aeecb1a542713ad6eda140f35

    bb7a87b Reticulate them splines (#123)

  Another example (a reversion in spirit)::

    f733e71 Revert reticulation of discombolutated splines (#123)

            bb7a87b
    …
    bb7a87b Reticulate them splines (#123)

Commit title tags
-----------------

* Commit titles can have tags. Tags appear between square brackets at the very
  beginning of a commit message. Multiple tags are separated by space. The
  following tags are defined:

  - ``u`` the commit requires following manual steps to upgrade a working copy
    or deployment. See `UPGRADING.rst`_ for details.

  - ``r`` the commit represents a change that requires reindexing a deployment
    after that commit is deployed there.

  - ``R`` the commit requires running ``make requirements`` after switching a
    working copy to a branch that includes that commit

  - ``M/N`` number of parts and ordinal of part in `Split commits`_

* Tags must appear in a title in the order they are defined above, as in
  ``[u r R 1/2]``. This ensures that more consequential tags appear earlier.

.. _UPGRADING.rst: ./UPGRADING.rst

Issue Tracking
==============

* We use Github's built-in issue tracking and ZenHub.

* We use `sentence case`_ for issue titles.

* We don't use a period at the end of issue titles.

* For issue titles we prefer brevity over precision or accuracy. Issue titles
  are read many times and should be optimized toward quickly scanning them.
  Potential omissions, inaccuracies and ambiguities in the title can be added,
  corrected or clarified in the description.

* We make liberal use of labels. Labels denoting the subject of an issue are
  blue, those denoting the kind of issue are green, issues relating to the
  development process are yellow. Important labels are red.

* We prefer issue to be assigned to one person at a time. If the original
  assignee needs the assistance by another team member, the issue should be
  assigned to the assisting person. Once assistance was provided, the ticket
  should be assigned back to the original assignee.

* We use ZenHub dependencies between issues to express constraints on the
  order in which those issues can be worked on.  If issue ``#1`` blocks
  ``#2``, then work on ``#2`` can't begin before work on ``#1`` has completed.
  For issues that are resolved by a commit, work is considered complete when
  that commit appears on the ``develop`` branch.

* Freebies: If the resolution to one issue implicitly resolves another one,
  that second issue is called a *freebie*. Freebies are assigned to the
  assignee of the primary issue and their estimate is set to zero. A freebie
  issue should also be marked as blocked by the *PR* that resolves it. A freebie
  is moved manually, through the ZenHub pipelines, in tandem with its
  respective primary issue. Freebie resolution is demonstrated independently.

  Freebies should be used sparingly. Preferably, separate issues are resolved
  in separate PRs. A commit that addresses a primary issue and a freebie have
  a title that lists them both e.g., ``Fix foo (#1, #2)``. 

  Note that dedicating a commit to a freebie on a PR branch is a bad smell. If
  the issue can be resolved in a separate commit, it may as well be resolved
  on a separate branch.


Pull Requests
=============

Naming Branches
---------------

* When naming PR branches we follow the template below::
  
    issues/$AUTHOR/$ISSUE_NUMBER-$DESCRIPTION
      
  ``AUTHOR`` is the Github profile name of the PR author.
  
  ``ISSUE_NUMBER`` is a numeric reference to the issue that this PR addresses.
  
  ``DESCRIPTION`` is a short (no more than nine words) slug_ describing the
  branch

Rebasing
--------

* The PR author rebases the PR branch before every review

Fixups
------

* Changes that address the outcome of a review should appear as separate commit.
  We prefix the title of those commits with ``fixup!`` and follow that with
  a space and the title of an earlier commit that the current commit should be
  squashed with. A convenient way to create those commits is by using the
  ``--fixup`` option to ``git commit``.

Squashing previous fixups
-------------------------

* Unless the PR reviewer has already done so, the PR author squashes all
  existing fixups after they get the branch back from the reviewer, and before
  addressing the review outcome with more fixups.


Assigning PRs
-------------

* The author of a PR may request reviews from anyone at any time. Once the
  author considers a PR ready to land (be merged into the base branch), the
  author rebases the branch, assigns the PR to the reviewer, the *primary
  reviewer* and requests a review from that person. Note that assigning a PR
  and requesting a review are different actions on the Github UI.

* If a PR is assigned to someone (typically the primary reviewer), only the
  assignee may push to the PR branch. If a PR is assigned to no one, only the
  author may push to the PR branch.

Rewriting history
-----------------

* Commits in a PR should not invalidate changes from previous commits in the PR.
  Revisions that occur during development should be incorporated into their
  relevant ancestor commit. There are various techniques to achieve this (``git
  commit --amend``, ``git rebase --interactive``, ``git rebase --interactive
  --autosquash`` or ``git reset`` and committing the changes again but all of
  these techniques involve rewriting the commit history. Rewriting the history
  of a feature branch is allowed and even encouraged but …

* … we only rewrite the part of the branch that has not yet been reviewed.
  To modify a commit that has already been reviewed, we create a new ``fixup!``
  commit containing the changes that addressing the reviewers comments.
  
  Before asking for another review, we may amend or rewrite that ``!fixup``
  commit. In fact, amending a ``!fixup`` commit between reviews is preferred in
  order to avoid a series of redundant fixup commits referring to the same main
  commit. In other words, the commits added to a feature branch after a review
  should all have distinct titles.

Drop commits
------------

* At times it may be necessary to temporarily add a commit to a PR branch e.g.,
  to facilitate testing. These commits should be removed prior to landing the
  PR and their title is prefixed with ``drop!``.
  
* When squashing old fixups, ``drop!`` commits should be be retained.

* Most PRs land squashed down into a single commit. A PR with more than one
  significant commit is referred to as a *multi-commit PR*. Prior to landing
  such a PR, the primary reviewer may decide to consolidate its branch.
  Alternatively, the primary reviewer may ask the PR author to do so in a final
  rejection of the PR. The final consolidation eliminates both ``fixup!`` and
  ``drop!`` commits.

Status checks
-------------

* We usually don't request a review before all status checks are green. In
  certain cases a preliminary review of a work in progress is permissible but
  the request for a preliminary review has to be qualified as such in a comment
  on the PR.

Holding branches warm
---------------------

* Some PR branches are can't be reviewed or merged for concerns external to the
  PR. The PR is labeled ``hold warm`` and the assignee of the PR, or the author,
  if no assignee is set, rebases the branch periodically and resolves any
  conflicts that might come up.

Merging
-------

* Without expressed permission by the primary reviewer, only the primary
  reviewer merges PR branches. Certain team members may possess sufficient
  privileges to push to main branches, but that does not imply that those team
  members may merge PR branches.
  
* The primary reviewer uses the ``sandbox`` label to indicate that a PR is
  being tested in the sandbox deployment prior to being merged. Only one open PR
  may be assigned the ``sandbox`` label at any point in time.
  
* When a PR branch is merged, the title of the merge commit should match the
  title of the pertinent commit in the branch, but also include the PR number.
  An example of this history looks like::

    *   8badf00d Reticulate them splines for good measure (#123, PR #124)
    |\
    | * cafebabe Reticulate them splines for good measure (#123)
    |/
    ...

  If a PR branch contains more than one commit, one of them usually represents
  the main feature or fix while other commits are preparatory refactorings or
  minor unrelated changes. The title of merge commit in this case usually
  matches that of the main commit.

Review comments
---------------

* Github lets any user with write access resolve comments to changes in a PR. We
  aren't that permissive. When the reviewer makes a comment, either requesting
  a change or asking a question, the author addresses the comment by either

  - making the requested changes and reacting to the comment with a thumbs-up 👍

  - or replying with a comment that answers the question or explains why the
    change can't be applied as requested.

  In either case, only the reviewer resolves the comment. This is to ensure that
  the reviewer can refresh their memory as to which changes they requested in a
  prior review so they can verify if they were addressed satisfactorily.

PR dependencies
---------------

* We use ZenHub dependencies between PRs to define constraints on the order in
  which they can be merged into ``develop``. If PR ``#3`` blocks ``#4``, then
  ``#3`` must be merged before ``#4``. Issues must not block PRs and PRs must
  not block issues. The only express relation we use between issues and PRs is
  ZenHub's *Link to issue* feature. Note that an explicit dependency between
  two issues implies a dependency between the PRs linked to the issues: if
  issue ``#1`` blocks issue ``#2`` and PR ``#3`` is linked to ``#1`` while PR
  ``#4`` is linked to ``#2``, then PR ``#4`` must be merged after ``#3``.

Chained PRs
-----------

* If two PRs touch the same code, one can be chained to the other in order to
  avoid excessive merge conflicts after one of them lands. The PR less likely to
  land soon should be chained to the other one.

* Similarly, if one PR depends on changes in another PR, the first PR may be
  chained to the second one so both can be worked on simultaneously.

* To chain PR ``#4`` to PR ``#3``

  1) Using ``git``, base the ``#4`` branch on the ``#3`` branch

  2) In Github, set the base of PR ``#4`` to the ``#3`` branch

  3) In Github, label ``#3`` as ``chain``

  4) In ZenHub, mark PR ``#4`` as blocked by PR ``#3``

  This allows the primary reviewer to break the chain when they merge ``#3``.
  The label catches their attention, the dependency lets them follow the chain
  and the target branch setting allows reviewers to ignore changes in the base
  branch.

* Rebasing a chained PR involves rebasing its branch on the base branch, instead
  of ``develop``.

* Once the base PR of a chain is merged, the chained PR needs to be rebased::

    git rebase --onto origin/develop $start_commit issues/joe/1234-foo

  where ``start_commit`` is the first commit in ``issues/joe/1234-foo`` that
  wasn't also on the base PR's branch.

* Travis does not build chained PRs by default. To fix this, modify
  ``branches.only`` in ``.travis.yml`` to list the name of the base branch instead
  of ``develop``. Commit that change with a title starting in ``drop!``. After
  the base PR lands, remove the ``drop`` commit.

.. _slug: https://en.wikipedia.org/wiki/Clean_URL#Slug
  

.. |ss| raw:: html

   <strike>

.. |se| raw:: html

   </strike>
