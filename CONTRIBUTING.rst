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
  used as long as consistency with surounding code is maintained.

* We prefer single quoted strings except in JSON literals.

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

* We don't use trailing commas in enumerations to optimize diffs (yet).

  .. note:: 
  
      If we were to adopt trailing commas, we would also have to abandon our
      preference of aligned indent.

* We avoid the use of backslash for continuing statements beyond one line.
  Instead, we exploity the fact that Python can infer continuation if they
  occur in balanced constructs like brackets or parentheses. If necessary we
  introduce a a pair of parentheses around the wrapping expression.

  With some keywords it is impossible to add semantically insignificant
  parentheses. For example, ``assert foo, "bad"`` is not equivalent to ``assert
  (foo, "bad")``. In these exceptional situations it is permissable to use
  backslash for line continuation.


Imports
*******

* We prefer absolute imports.

* We sort imports first by category, then lexicographically by module name and
  then by imported symbol. The categories are

  1. Import of modules in the Python runtime
    
  2. Imports of modules in external dependencies of the project
    
  3. Imports of modules in the project

* We always wrap `from` imports of more than one symbol, using a pair of
  parentheses around the list of symbols::

    from foo import (x,
                     y

  .. note:: 
  
      This isn't sufficiently thought through. The motivation behind wrapping
      always is to make diffs smaller and reduce the potential for merge
      conflicts. The use of aligned indent, however, counteracts that. There is
      also the question of whether single-symbol `from` imports should also use
      parentheses and wrapping such that every addition of an imported symbol
      is a one-line diff. We could also discourage multi-symbol `from` imports
      and require that every symbol is imported in a seperate statement.


Comments
********

* We don't use inline comments to explain what should be obvious software
  engineers familiar with the project. To help new contributors become
  familiar, we document the project architecture and algorithms separately from
  the Python source code.

* When there is the need to explain in the source, we focus on the Why rather
  than the How.


Inline documentation
********************

* We use docstrings to document the purpose of an artifact (module, class,
  function or method), and its contract between it and client code using it. We
  don't specify implementation details in docstrings.

* We put the triple quotes that delimit docstrings on separate lines::

    def foo():
        """
        Does nothing.
        """
        pass
        
  This visually separates function signature, docstring and function body from
  each other.


Code hygiene
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
  introduce semantic changes and it should precede the commit that resovles the
  issue.

Type hints
**********

* We use type hints both to document intent and to facilitate type checking by
  the IDE as well as additional tooling.
  
* When defining type hints for a function or method, we do so for all its
  parameters and return values.
  
* We prefer the generic types from ``typing`` over those from the
  ``collections`` module e.g., ``Mapping[K,V]`` over ``dict``.

* Due to the prominence of JSON in the project we annotate variables containing
  deserialized JSON as such, using the ``JSON`` type from ``azul.typing``.
  


Version Control
===============

* Feature branches are integrated by rebasing or squashing. We only use merges
  between deployment branches, either to promote changes in their natural
  progression from one deployment to the next or to

* We commit independent changes separately. If two changes could be applied in
  either order, they should occur in separate commits. Two changes A and B of
  which B depends on A may still be comitted separately if B represents an
  extension of A that we might want to revert while leaving A in place.

* In theory, every individual commit should pass unit and integration tests. In
  practice, on PR branches with long histories not intented to be squashed, not
  every commit is built in CI. This is acceptable.

  .. note::

    I am not a fan the above rule but the desire to maintain a linear history
    by rebasing PR branches as opposed to merging them demands that loophole.
    When pushing a rebased PR branch, we'd have to build every commit on that
    branch individually. The loophole can be avoided by creating narrowly
    focused PRs with only one logical change and therefore one commit.
    Therefore, we consider the creation of PRs with longer histories to be a
    privilege of trusted, long-time contributors. See ``


* If a commit resolves (or contributes to the resolution of) an issue, we
  mention that issue at the end of the commit title::

    Reticulate them splines for good measure (#123)

  Note that we don't use Github resolution keywords like "fixes" or "resolves".
  Any mention of those preceding an issue reference in a title would
  automatically close the issue as soon as the commit lands on the default
  branch. This is undesirable as we want to continue to track issues in
  Zenhub's *Merged* and *Done* pipelines even after the commit lands on
  `develop`.

* We value `expressive and concise commit message titles`_ and we use Github's
  limit on the length of a commit message title. A commit title is too long if
  (and only if) Github truncates or wraps the title in its UI.

* We don't use a period at the end of commit titles.

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
  ``SQ:`` an follow that with the title of an earlier commit that the current
  commit should be squashed with. 
  
* At times it may be necessary to temporarily add a commit to a PR branch e.g.,
  to facilitate testing. These commits should be removed prior to landing the
  PR and their title is prefixed with ``DELETE ME:``.
  
* We periodically consolidate long branches to simplify conflict resolution
  during rebasing. Consolidation means squashing ``SQ:`` commits so they
  disappear from the history. ``DELETE ME:`` commits should be retained.

* Most PRs land squashed down into a single commit. A PR with more than one
  significant commit is referred to as a *multi-commit PR*. Prior to landing
  such a PR, the lead may decide to consolidate its branch. Alternatively, the
  lead may ask the PR author to do so in a final rejection of the PR. The final
  consolidation eliminates both ``SQ:`` and ``DELETE ME:`` commits.

* We usually don't request a review before all status checks are green. In
  certain cases a preliminary review of a work in progress is permissable.
  
* Without expressed permission by the lead, only the lead lands PR branches.
  Even if certain team members posess sufficient privileges to push to the main
  branches, that does not imply that those team members may land PR branches.
  
* We use a special label `sandbox` to indicate that a PR is being tested in the
  sandbox deployment prior to being merged. Only one open PR may be assigned
  the `sandbox` label.
  
.. _slug: https://en.wikipedia.org/wiki/Clean_URL#Slug
  

