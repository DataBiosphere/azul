# FIXME: https://github.com/DataBiosphere/azul/issues/4824
#        Move script to attic once the import has been completed

from more_itertools import (
    one,
)

from azul import (
    config,
    logging,
)
from azul.deployment import (
    aws,
)
from azul.logging import (
    configure_script_logging,
)
from azul.terraform import (
    terraform,
)

log = logging.getLogger(__name__)


def main() -> None:
    if config.terraform_component == 'gitlab':
        import_gitlab_security_group()
    else:
        log.info("The 'gitlab' component is not selected, skipping import.")


def get_vpc_id(name: str) -> str:
    filters = [
        {
            'Name': 'tag:Name',
            'Values': [name]
        }
    ]
    vpcs = aws.ec2.describe_vpcs(Filters=filters)
    return one(vpcs['Vpcs'])['VpcId']


def get_default_security_group_id(vpc_id: str) -> str:
    filters = [
        {
            'Name': 'vpc-id',
            'Values': [vpc_id]
        },
        {
            'Name': 'group-name',
            'Values': ['default']
        }
    ]
    groups = aws.ec2.describe_security_groups(Filters=filters)
    return one(groups['SecurityGroups'])['GroupId']


def import_gitlab_security_group() -> None:
    resources = terraform.run('state', 'list').splitlines()

    resource = 'aws_default_security_group.gitlab'
    if resource in resources:
        log.info("Default security group of the 'azul-gitlab' VPC has already been imported.")
    else:
        vpc_id = get_vpc_id(name='azul-gitlab')
        group_id = get_default_security_group_id(vpc_id=vpc_id)
        log.info("Importing the default security group of the 'azul-gitlab' VPC …")
        terraform.run('import', resource, group_id)


if __name__ == '__main__':
    configure_script_logging()
    main()
