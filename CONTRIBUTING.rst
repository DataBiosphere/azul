Contributing
------------

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

* For Python we use PEP8 with E722 disabled (type too general in except clause)
  and the maximum line length set to 120 characters.

* For documentation we use a maximum line length of 80 characters. For comments
  and docstrings in Python, we prefer a line length of 80, but 120 may also be
  used as long as consistency with surrounding code is maintained.

* We prefer single quoted strings except in JSON literals.

* We avoid backslash escaping quote characters within a string by using double
  quoted strings when the string contains a single quote character (and vice
  versa). https://www.python.org/dev/peps/pep-0008/#string-quotes

* We prefer aligned indent for wrapped constructs except for literal
  collections such as dictionaries, lists, tuples and sets::

    self.call_me(positional,
                 x=1,
                 y=2)

    foo = {
        "foo": False,
        "a": [1, 2, 3]
    }

* Small literal collections may be kept on one line up to the maximum line
  length. A small collection is one that has no more than 9 elements, all of
  which either primitive values or other small collections.

* We wrap all elements or none. The following is discouraged::

    self.call_me(foo, bar,
                 x=1, y=2)

  The one exception to this rule are logging method invocations. The first
  argument, the log message, is excluded from this rule::

    logger.info('Waiting up to %s seconds for %s queues to %s ...',
                timeout, len(queues), 'empty' if empty else 'not be empty')

  Only if the second and subsequent arguments won't fit on one line, do we
  wrap all arguments, one line per argument.

* We don't use trailing commas in enumerations to optimize diffs yet. [#]_

* We avoid the use of backslash for continuing statements beyond one line.
  Instead, we exploit the fact that Python can infer continuation if they
  occur in balanced constructs like brackets or parentheses. If necessary we
  introduce a a pair of parentheses around the wrapping expression.

  With some keywords it is impossible to add semantically insignificant
  parentheses. For example, ``assert foo, "bad"`` is not equivalent to ``assert
  (foo, "bad")``. In these exceptional situations it is permissible to use
  backslash for line continuation.

.. [#] Note: If we were to adopt trailing commas, we would also have to
       abandon our preference of aligned indent.

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
    
* We use ``str.join()`` when joining more than three elements with the same
  character or when the elements are already in an iterable form::
  
    f'{a},{b},{c},{d}'     # while this is shorter
    ','.join((a, b, c, d)) # this is more readable
  
    f'{a[0],a[1]}  # this is noisy and tedious
    ','.join(a)    # this is not
 
* We use `EAFP`_ as a principle.

.. _EAFP: https://stackoverflow.com/questions/11360858/what-is-the-eafp-principle-in-python

Logging
*******

* Loggers are instantiated in every module that needs to log

* Loggers are always instantiated as follows::

    log = logging.getLogger(__name__) # is preferred for new code
    logger = logging.getLogger(__name__) # this is ok in old code
  
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


Imports
*******

* We prefer absolute imports. [#]_

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

    ^3wi(<ENTER><ESCAPE>A,<ENTER>)

.. [#] Note: PEP8 recommends instead of mandating them. Rather than defining
       the circumstances under which relative imports are acceptable or even
       desirable, I'd like to keep the rules simple. The rare cases in which
       relative imports are beneficial—they minimize the diff when moving a
       package and they can be used to shorten long import paths—don't pay for
       the complexity that allowing them would add to these rules.

       I have also seen PyCharm mess up refactoring relative imports. I also
       find the mixing relative with absolute imports—which inevitably occurs
       in all but the most simple modules—to be visually noisy.

Comments
********

* We don't use inline comments to explain what should be obvious to software
  engineers familiar with the project. To help new contributors become
  familiar, we document the project architecture and algorithms separately from
  the Python source code in a ``docs`` subdirectory of the project root. 

* When there is the need to explain in the source, we focus on the Why rather
  than the How.


Inline Documentation
********************

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
  

Code Hygiene
************

* We avoid duplication of code and continually refactor it with the goals of
  reducing entropy while increasing consistency and reuse.

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
  
* We generally use top-down ordering of artifacts within a module or script.
  Helper and utility artifacts succeed the code that use them. Bottom-up
  ordering—which has the elementary building blocks occur first—makes it harder
  to determine the purpose and intent of a module at a glance.
  
* To temporarily disable a section of code, we embed it in a conditional
  statement with an test that always evaluates to false (``if False:`` in
  Python) instead of commenting that section out. We do this to keep the code
  subject to refactorings and code inspection tools.
  
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

    try:
        <do something>
    except:
        if <condition>:
           raise
        else:
           pass


  While neither ``else`` nor ``pass`` are semantically required, including them
  anyway expresses the author's intent more strongly, eliminating all doubt in
  a potential reviewer about whether the author considered the case in which
  the condition is false.
  
  Similarly,
  
  ::
  
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
     

Type Hints
**********

* We use type hints both to document intent and to facilitate type checking by
  the IDE as well as additional tooling.
  
* When defining type hints for a function or method, we do so for all its
  parameters and return values.
  
* We prefer the generic types from ``typing`` over non-generic ones from the
  ``collections`` module e.g., ``MutableMapping[K,V]`` or ``Dict[K,V]`` over
  ``dict``. For method/function arguments we prefer the least specific type
  possible e.g., ``Mapping`` over ``MutableMapping`` over ``Dict``. For
  example, we don't use ``MutableMapping`` for an argument unless it is
  actually modified by the function/method. For return values we specify the
  type that we anticipate to be useful by the caller without being overly
  specific. For example, we prefer ``MutableMapping`` for the return type
  because ``Mapping`` would prevent the caller from modifying the returned
  dictionary, something that's typically not desirable. If we do want to
  prevent modification we would return a ``frozendict`` or equivalent and
  declare the return value as ``Mapping``. Even if the concrete type of the
  return value is ``dict``, we don't use ``Dict`` for the type hint because it
  might limit future changes to the concrete type of the return value and
  that's something we want to avoid, especially in externally facing APIs where
  backwards compatibility is a more important concern.

* Owing to the prominence of JSON in the project we annotate variables
  containing deserialized JSON as such, using the ``JSON`` type from
  ``azul.typing``. Note that due to the lack of recursive types in PEP-484,
  ``JSON`` unrolls the recursion only three levels deep. This means that with
  ``x: JSON`` the expression ``x['a']['b']['c']`` would be of type ``JSON``
  while ``x['a']['b']['c']['d']`` would be of type ``Any``.

  
Testing
=======

* All code should be covered by unit tests.

* Legacy code for which tests were never written should be covered when it is
  modified.
  
* Combinatorial tests (tests that exercise a number of combinations of inputs)
  should make use of ``unittest.TestCase.subTest()`` so a single failing
  combination doesn't prevent other combinations form being exercised.

* Code that doesn't require elaborate or expensive fixtures should use doctests
  if that adds clarity to the documentation or helps with expressing intent.
  Modules containing doctests must be registered in the ``test_doctests.py``
  script.
  
* Code that can only be tested in a real deployment should be covered by an
  integration test.


Version Control
===============

* Feature branches are merged into ``develop``. If a hotfix is made to a
  deployment branch other than ``develop``, that branch is also merged into
  ``develop`` so that the hotfix eventually propagates to all deployments.

* During a promotion, the branch for a lower deployment (say, ``integration``)
  is merged into the branch for the next higher deployment.

* We commit independent changes separately. If two changes could be applied in
  either order, they should occur in separate commits. Two changes A and B of
  which B depends on A may still be committed separately if B represents an
  extension of A that we might want to revert while leaving A in place.

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

* If a commit resolves (or contributes to the resolution of) an issue, we
  mention that issue at the end of the commit title::

    Reticulate them splines for good measure (#123)

  Note that we don't use Github resolution keywords like "fixes" or "resolves".
  Any mention of those preceding an issue reference in a title would
  automatically close the issue as soon as the commit appears on the default
  branch. This is undesirable as we want to continue to track issues in
  Zenhub's *Merged* and *Done* pipelines even after the commit appears on the
  ``develop`` branch.

* We value `expressive and concise commit message titles`_ and we use Github's
  limit of 72 characters for the length of a commit message title. Beyond 72
  characters, Github truncates the title at 69 characters and adds three dots
  (ellipsis) which is undesirable. Titles with lots of wide characters like
  ``W`` may still wrap (as opposed to being truncated) but that's improbable
  and therefor acceptable.

* We don't use a period at the end of commit titles because |ss| I dislike it
  |se| Github usually only renders the title and most commonly renders a title
  alongside the titles of other commits (and so do many Git GUIs) which
  effectively turns the title into an item in a list. There is no point in
  ending every item in a list with a period, pun intended.

* We use `sentence case`_ for commit titles.

.. _expressive and concise commit message titles: https://chris.beams.io/posts/git-commit/

.. _sentence case: https://utica.libguides.com/c.php?g=291672&p=1943001


Issue Tracking
==============

* We use Github's builtin issue tracking and Zenhub.

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

* We use Zenhub dependencies between issues to express constraints on the
  order in which those issues can be worked on.  If issue ``#1`` blocks
  ``#2``, then work on ``#2`` can't begin before work on ``#1`` has completed.
  For issues that are resolved by a commit, work is considered complete when
  that commit appears on the ``develop`` branch.

* Freebies: If the resolution to one issue implicitly resolves another one,
  that second issue is called a *freebie*. Freebies are assigned to the
  assignee of the primary issue and their estimate is set to zero. A freebie
  issue should also be marked as blocked by the *PR* that resolves it. A freebie
  is moved manually, through the Zenhub pipelines, in tandem with its
  respective primary issue. Freebie resolution is demonstrated independently.

  Freebies should be used sparingly. Preferably, separate issues are resolved
  in separate PRs. A commit that addresses a primary issue and a freebie have
  a title that lists them both e.g., ``Fix foo (#1, #2)``. 

  Note that dedicating a commit to a freebie on a PR branch is a bad smell. If
  the issue can be resolved in a separate commit, it may as well be resolved
  on a separate branch.


Pull Requests
=============

* When naming PR branches we follow the template below::
  
    issues/$AUTHOR/$ISSUE_NUMBER-$DESCRIPTION
      
  ``AUTHOR`` is the Github profile name of the PR author.
  
  ``ISSUE_NUMBER`` is a numeric reference to the issue that this PR addresses.
  
  ``DESCRIPTION`` is a short (no more than nine words) slug_ describing the
  branch

* We rebase PR branches daily but …

* … we don't eagerly squash them. Changes that address the outcome of a review
  should appear as separate commit. We prefix the title of those commits with
  ``fixup! `` and follow that with the title of an earlier commit that the
  current commit should be squashed with. A convenient way to create those
  commits is by using the ``--fixup`` option to ``git commit``.
  
* The author of a PR may request reviews from anyone at any time. Once the
  author considers a PR ready to land (be merged into the base branch), the
  author rebases the branch, assigns the PR to the reviewer, the *primary
  reviewer* and requests a review from that person. Note that assigning a PR
  and requesting a review are different actions on the Github UI.

* If a PR is assigned to someone (typically the primary reviewer), only the
  assignee may push to the PR branch. If a PR is assigned to no one, only the
  author may push to the PR branch.

* Commits in a PR should not invalidate changes from previous commits in the PR.
  Revisions that occur during development should be incorporated into their
  relevant ancestor commit. There are various techniques to achieve this (``git
  commit --amend``, ``git rebase --interactive``, ``git rebase --interactive
  --autosquash`` or ``git reset`` and committing the changes again but all of
  these techniques involve rewriting the commit history. Rewriting the history
  of a feature branch is allowed and even encouraged but …

* … we only rewrite the part of the branch that has not yet been been reviewed.
  To modify a commit that has already been reviewed, we create a new ``fixup!``
  commit containing the changes that addressing the reviewers comments.
  
  Before asking for another review, we may amend or rerwrite that ``!fixup``
  commit. In fact, amending a ``!fixup`` commit between reviews is preferred in
  order to avoid a series of redundant fixup commits referring to the same main
  commit. In other words, the commits added to a feature branch after a review
  should all have dictinct titles.
  
  Considering that we also require frequent rebasing, this rule makes for a
  more transparent review process. The reviewers can ignore force pushes
  because those can only be the result of rebases or in-between review amends.
  The reviewer can still see a record of the changes made in response to
  previous review comments and how those changes affected the build status of
  the PR.
  
* At times it may be necessary to temporarily add a commit to a PR branch e.g.,
  to facilitate testing. These commits should be removed prior to landing the
  PR and their title is prefixed with ``drop!``.
  
* The reviewer may ask the author to consolidate long PR branches in order to
  simplify conflict resolution during rebasing. Consolidation means squashing
  ``fixup!`` commits so they disappear from the history. ``drop!`` commits
  may be retained during consolidation.

* Most PRs land squashed down into a single commit. A PR with more than one
  significant commit is referred to as a *multi-commit PR*. Prior to landing
  such a PR, the primary reviewer may decide to consolidate its branch.
  Alternatively, the primary reviewer may ask the PR author to do so in a final
  rejection of the PR. The final consolidation eliminates both ``fixup!`` and
  ``drop!`` commits.

* We usually don't request a review before all status checks are green. In
  certain cases a preliminary review of a work in progress is permissible but
  the request for a preliminary review has to be qualified as such in a comment
  on the PR.
  
* Without expressed permission by the primary reviewer, only the primary
  reviewer integrates PR branches. Certain team members may possess sufficient
  privileges to push to main branches, but that does not imply that those team
  members may land PR branches.
  
* The primary reviewer uses the ``sandbox`` label to indicate that a PR is
  being tested in the sandbox deployment prior to landing. Only one open PR may
  be assigned the ``sandbox`` label at any point in time.
  
* Until further notice only the lead may act as a primary reviewer.

* Feature branches are integrated by merging. The title of the merge commit
  should match the title of the pertinent commit in the branch, but also include
  the PR number. An example of this history looks like::

    *   8badf00d Reticulate them splines for good measure (#123, PR #124)
    |\
    | * cafebabe Reticulate them splines for good measure (#123)
    |/
    ...

  If a feature branch contains more than one commit, one of them usually
  represents the main feature or fix while other commits are preparatory
  refactorings or minor unrelated changes. The title of merge commit in this
  case usually matches that of the main commit.

* We use Zenhub dependencies between PRs to define constraints on the order in
  which they can be merged into ``develop``. If PR ``#3`` blocks ``#4``, then
  ``#3`` must be merged before ``#4``. Issues must not block PRs and PRs must
  not block issues. The only express relation we use between issues and PRs is
  Zenhub's *Link to issue* feature. Note that an explicit dependency between
  two issues implies a dependency between the PRs linked to the issues: if
  issue ``#1`` blocks issue ``#2`` and PR ``#3`` is linked to ``#1`` while PR
  ``#4`` is linked to ``#2``, then PR ``#4`` must be merged after ``#3``.

* Chained PRs: If two PRs touch the same area of code, they can be chained to
  avoid  excessive merge conflicts. To chain PR ``#3`` and ``#4``, base the
  source branch for ``#4`` on that for ``#3``, set the target branch of ``#4``
  to the source branch of ``#3``, label ``#3`` as ``chain`` and mark ``#4`` as
  blocked by ``#3``.  This allows the primary reviewer to break the chain when
  they merge ``#3``. The label catches their attention, the dependency lets
  them follow the chain and the target branch setting allows reviewers to
  ignore changes in the base branch. Note that you'd typically chain PRs if
  their issues are independent: if they were dependent, they shouldn't be
  worked on simultaneously.


.. _slug: https://en.wikipedia.org/wiki/Clean_URL#Slug
  

.. |ss| raw:: html

   <strike>

.. |se| raw:: html

   </strike>
