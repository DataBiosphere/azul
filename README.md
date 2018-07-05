The Azul project contains the components that together serve as the backend to
Boardwalk, a web application for browsing genomic data sets. 

Azul consists of two components: an indexer and a web service. The Azul indexer
is an AWS Lambda function that responds to webhook notifications about bundle
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
