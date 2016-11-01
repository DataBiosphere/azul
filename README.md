The Azul project contains the components that together serve as the backend to
Boardwalk, a web application for browsing genomic data sets. 

Azul consists of an indexer and a web-service. The Azul indexer is an AWS
Lambda functiomn responding to web hook notifications for bundle addition and
deletion events in a data store, It responds to those notifications by
retrieving the bundle's metadata from said data store, transforming it and
writing the transformed metadata into an Elasticsearch index. The
transformation extracts selected entities and denormalizes their relations into
a document shape that facilitates efficient queries on a finite but
customizable set of metadata facets.

The Azul web service, another AWS Lambda function, serves as a thin translation
layer between Elasticsearch and the Boardwalk UI, providing features like
custom authentication and customizable field name translations as well as
introspective capabilities such as facet and entity type discovery.

Both the indexer and the web service allow for project-specific customizatons
via a plug-in mechanism, allowing the Boardwalk UI codebase to be functionally
generic with minimal need for project-specific behavior.
