#!/bin/bash
while [ -z $( $1/bin/aws es --profile $2 describe-elasticsearch-domain --domain-name $3 | grep "Endpoint" ) ]; do
	sleep 120
done
echo $( $1/bin/aws es --profile $2 describe-elasticsearch-domain --domain-name $3 | jq ".DomainStatus.ARN" )
echo $( $1/bin/aws es --profile $2 describe-elasticsearch-domain --domain-name $3 | jq ".DomainStatus.Endpoint" )