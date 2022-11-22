from more_itertools import (
    one,
)

from azul import (
    cached_property,
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
    vpc,
)

log = logging.getLogger(__name__)


class ImportDefaultVPC:

    def main(self) -> None:
        if config.terraform_component == 'shared':
            self.import_default_vpc()
        else:
            log.info("The 'shared' component is not selected, skipping import.")

    @cached_property
    def default_vpc_id(self) -> str:
        filters = [
            {
                'Name': 'isDefault',
                'Values': ['true']
            }
        ]
        vpcs = aws.ec2.describe_vpcs(Filters=filters)
        return one(vpcs['Vpcs'])['VpcId']

    def default_security_group_id(self, vpc_id: str) -> str:
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

    def import_default_vpc(self) -> None:
        resources = terraform.run('state', 'list').splitlines()

        resource = 'aws_default_vpc.' + vpc.default_vpc_name
        if resource in resources:
            log.info('The default VPC has already been imported.')
        else:
            log.info('Importing the default VPC …')
            terraform.run('import', resource, self.default_vpc_id)

        resource = 'aws_default_security_group.' + vpc.default_security_group_name
        if resource in resources:
            log.info("The default VPC's default security group has already been imported.")
        else:
            group_id = self.default_security_group_id(self.default_vpc_id)
            log.info("Importing the default VPC's default security group …")
            terraform.run('import', resource, group_id)


if __name__ == '__main__':
    configure_script_logging()
    ImportDefaultVPC().main()
