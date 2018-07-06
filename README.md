The Azul project contains the components that together serve as the backend to
Boardwalk, a web application for browsing genomic data sets. 

Azul consists of two components: an indexer and a web service. The Azul indexer
is an AWS Lambda function that responds to web-hook notifications about bundle
addition and deletion events occurring in a [data
store](https://github.com/HumanCellAtlas/data-store) instance. The indexer
responds to those notifications by retrieving the bundle's metadata from said
data store, transforming it and writing the transformed metadata into an
Elasticsearch index. The transformation extracts selected entities and
denormalizes the relations between them into a document shape that facilitates
efficient queries on a number of customizable metadata facets.

The Azul web service, another AWS Lambda function fronted by API Gateway,
serves as a thin translation layer between Elasticsearch and the Boardwalk UI,
providing features like pluggable authentication, field name translation and
introspective capabilities such as facet and entity type discovery.

Both the indexer and the web service allow for project-specific customizations
via a plug-in mechanism, allowing the Boardwalk UI codebase to be functionally
generic with minimal need for project-specific behavior.


Branch flow & development process
=================================

The section below describes the flow we want to get to eventually, not the one
we are currently using while this repository recovers from the aftermath of its
inception.

The declared goal here is a process that prevents diverging forks yet allows
each project to operate independently as far as release schedule, deployment
cadence, project management and issue tracking is concerned. The main
challenges are 1) preventing contention on a single `develop` or `master`
branch, 2) isolating project-specific changes from generic ones, 3) maintaining
a reasonably linear and clean history and 4) ensuring code reuse.

The [original repository](https://github.com/DataBiosphere/azul), also known as
*upstream*, should only contain generic functionality and infrastructure code.
Project-specific functionality should be maintained in separate
project-specific forks of that repository. The upstream repository will only
contain a `master` branch and the occasional PR branch.

Azul dynamically imports project-specific plugin modules from a special
location in the Python package hierarchy: `azul.projects`. The package
structure in upstream is

```
root
├── ...
├── src
│   └── azul
│       ├── index
│       │   └── ...
│       ├── projects (empty)
│       ├── service
│       │   └── ...
│       └── util
│       │   └── ...
└── ...
```

Note that the `projects` directory is empty. 

The directory structure in forked repositories is generally the same with one
important difference. While a fork's `master` branch is an approximate mirror
of upstream's `master` and therefore also lacks content in `projects`, that
directory *does* contain modules in the fork's `develop` branch. In
`HumanCellAtlas/azul-hca`, the fork of Azul for the HumanCellAtlas project, the
`develop` branch would look like this:


```
root
├── ...
├── src
│   └── azul
│       ├── index
│       │   └── ...
│       ├── projects
│       │   └── hca
│       │       └── ...
│       ├── service
│       │   └── ...
│       └── util
│       │   └── ...
└── ...
```

The `develop` branch would only contain changes to the `azul.projects.hca`
package. All other changes would have to be considered generic—they would occur
on the fork's `master` branch and eventually be merged into upstream's `master`
branch. The `master` branches in each fork should not be divergent for
sustained periods of time while the project-specific branches can and will be.

The reason why each fork maintains a copy of the `master` branch is that forks
generally need to have a place to test and evaluate generic features before
they are promoted upstream. If there wasn't a `master` branch in a fork, the
project-specific `develop` branch in that fork would inevitably conflate
project-specific changes with generic ones. It would be very hard to
selectively promote generic changes upstream, even if the generic changes were
separate commits. 

The flow presented here establishes an easy-to-follow rule: If you're modifying
`azul.projects.hca`, you need to do so in a PR against `develop`. If you're
modifying anything else, you need to do so in a PR against `master`. The figure
below illustrates that.

```                                                                                 
                                                      ●────● feature/generic-foo
                                                     ╱                          
                                              4     ╱                           
    ─────●────────────────────────────────────●────●──────────────        master
          ╲                                  ╱                                  
 azul      ╲                                ╱                                   
 ─ ─ ─ ─ ─ ─╲─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ╱ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
 azul-hca    ╲                            ╱                                     
              ╲                          ╱                                      
    ──────●────●────●────●────●────●────●──────────────────────────       master
           ╲   1     ╲    ╲   A'   B'                                           
            ╲         ╲    ╲                                                    
             ╲         ╲    ●────● feature/master/generic-stuff                
              ╲         ╲   A    B                                              
               ╲         ╲                                                      
                ●─────────●─────────────●────●────●─────────────────     develop
                2         3              ╲   C'   D'                        
                                          ╲                                 
                                           ●────● feature/develop/specific-stuff
                                                C    D                           
```

Merge commit 1 from the upstream `master` branch integrates upstream changes
into the fork. These may be generic changes merged upstream from other forks or
changes that were directly PR-ed against `master` in upstream. Commit 2 marks
the beginning of the `develop` branch, adding the `azul.projects.hca` package.
Merge commit 3 brings the changes from commit 1 into the `develop` branch.

Another important rule is that collaborative branches like `develop` and
`master` are never rebased. Changes are exchanged between them using merge
commits instead. Individual branches however, like feature branches, are always
rebased onto the base branch. In the above example,
`feature/master/generic-stuff` is first rebased onto `master`, creating commits
A' and B'. Later those changes are merged upstream via commit 4. Both the
rebase and the merge happen via a pull request, but the landing action will be
"Rebase and merge" for the first PR and "Create a merge commit" for the second.

The reason for this distinction is that rebasing usually triggers more rebasing
of branches that were based on the rebased branch. It also rewrites the commit
timestamps, thereby obfuscating the history to some extent. For these two
reasons, rebasing is not a sustainable practice for collaborative branches. For
individual branches however, rebasing is possible because feature branches are
typically not used as a base for other branches. Rebasing is also desirable
because it produces a cleaner, linear history and we should use it whenever
possible. The back and forth merging between collaborative branches produces a
history that's somewhat convoluted so it is important to keep the history as
clean as possible in between merges.

Generic changes don't have to be conceived in a fork. We can also PR them
directly against the upstream repository as illustrated by branch
`feature/generic-foo`.

The most common type of pull request in a fork is one against that fork's
`develop` branch, `feature/develop/specific-stuff` for example. Note that
changes occurring on `develop` are never merged upstream.

As mentioned before, merge commit 4 is done via a pull request against the
upstream repository. It is possible and perfectly acceptable that such upstream
PRs combine multiple unrelated changes. They should be requested by the team
lead for the forking project and reviewed by an upstream lead. Shortly after
the PR lands, the requesting lead should perform a fast-forward merge of the
upstream `master` branch into the fork's `master` branch. This will propagate
the merge commit downstream before any subsequent commits occurring on fork's
`master` have a chance to complicate the history by introducing the infamous
merge of merge commits.

```
$ git branch
* master
  develop
$ git merge --ff-only upstream/master
Updating 450b0c0..212003c
Fast-forward
```

This procedure requires that the lead's local clone of the fork be set up with
two remotes: `origin` (the forked repository) and `upstream` (the upstream
repository). Other team members can usually get by with just one remote,
`origin`.

Deployment branches
-------------------

The code in the upstream repository should never be deployed anywhere because
it does not contain any concrete modules to be loaded at runtime. The code in a
fork, however, is typically active in a number of deployments. The specifics
should be left to each project but the rule of thumb should be that each
deployment corresponds to a separate branch in the fork. The `azul-hca` fork
has four deployments: development, integration, staging and production. The
development deployment, or `dev`, is done from the `develop` branch. Whenever a
commit is pushed to that branch, a continuous deployment script deploys the
code to AWS. The other deployment branches are named accordingly. Changes are
promoted between deployments via a merge. The merge is likely going to be a
fast-forward. A push to any of the deployment branches will trigger a CI/CD
build that performs the deployment. The promotion could be automatic and/or
gated on a condition, like tests passing.
