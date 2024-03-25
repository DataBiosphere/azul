import argparse
import logging
import re
import sys
from more_itertools import one

from azul.deployment import (
    aws
)
log = logging.getLogger('azul.gitlab.vpc_acl')

ec2 = aws.ec2


def determine_available_rule_num(entries: list[dict]) -> int:
    # When adding a CIDR to a blocked rule, we care only about determining
    # the next available inbound rule number less than 100.
    ingres_rules = {e['RuleNumber'] for e in entries if e['Egress'] is False}
    for i in range(len(ingres_rules)):
        i += 1
        if i not in ingres_rules:
            return i


def add_cidr_to_vpc_acl(acl_name: str, cidr: str):
    acl_id, entries = acl_info(acl_name)
    response = ec2.create_network_acl_entry(
        CidrBlock=cidr,
        Egress=False,  # Ingress rule
        NetworkAclId=acl_id,
        Protocol='-1',  # All protocols
        RuleAction='deny',
        RuleNumber=determine_available_rule_num(entries)
    )


def acl_info(acl_name: str):
    network_acl = _lookup_acl_by_vpc_name(acl_name)
    acl_id = network_acl['NetworkAclId']
    entries = network_acl['Entries']
    return acl_id, entries


def remove_cidr_from_vpc_acl(acl_name: str, cidr: str):
    # Get the Network ACL ID based on the ACL name
    acl_id, entries = acl_info(acl_name)
    matched = [e for e in entries if e['CidrBlock'] == cidr]
    if matched:
        matched = one(matched)
        # Remove the IP CIDR from the Network ACL
        response = ec2.delete_network_acl_entry(
            Egress=False,  # Ingress rule
            NetworkAclId=acl_id,
            RuleNumber=matched['RuleNumber'],  # Choose the same rule number used when adding the CIDR
        )
    else:
        log.warning('CIDR %s, does not match any actively managed CIDRs in this ACL', cidr)


def validate_ip_cidr(s: str) -> str:
    if re.match(''.join([
        '^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}',
        '(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)(/\\d{1,2})$']),
            s):
        return s
    else:
        log.error('Invalid CIDR format in %s, use CIDR notation (e.g. 10.0.0.0/16)', s)
        raise ValueError('Invalid CIDR format in %s', s)


def _lookup_acl_by_vpc_name(vpc_name: str) -> dict:
    vpc = ec2.describe_vpcs(Filters=[{'Name': 'tag:Name', 'Values': [vpc_name]}])
    vpc = one(vpc['Vpcs'])
    response = ec2.describe_network_acls(
        Filters=[
            {
                'Name': 'vpc-id',
                'Values': [vpc['VpcId']]
            }
        ]
    )
    return one(response['NetworkAcls'])


def list_ips_in_vpc_acl(acl_name: str, *d) -> None:
    network_acl = _lookup_acl_by_vpc_name(acl_name)
    acl_entries = sorted(network_acl['Entries'], key=lambda e: e['Egress'])
    keys = acl_entries[0].keys()
    list_output(acl_entries, keys)


def list_output(acl_entries, keys) -> None:
    header = '\t'.join(keys)
    print(header)
    print('-' * (len(header) + len(keys) * 4))
    for entry in acl_entries:
        protocol = entry['Protocol']
        entry['Protocol'] = 'Any' if protocol == '-1' else protocol
        rule_number = entry['RuleNumber']
        entry['RuleNumber'] = '\t' + ('*' if rule_number >= 32767 else str(rule_number))
        entry['RuleAction'] = '\t' + entry['RuleAction']
        print('\t'.join([str(entry[k]) for k in keys]))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)+7s %(name)s: %(message)s')

    p = argparse.ArgumentParser(description='Manage the GitLab VPC ACL to add or remove targeted CIDRs')
    sps = p.add_subparsers(help='sub-command help', dest='command')

    actions = {
        'add': add_cidr_to_vpc_acl,
        'remove': remove_cidr_from_vpc_acl,
        'list': list_ips_in_vpc_acl
    }
    for s in actions:
        sp = sps.add_parser(s,
                            help=f'Manage the GitLab VPC ACL to {s} the specified CIDR'
                            if s != 'list' else
                            f'List the managed CIDRs in the GitLab VPC ACL')
        sp.add_argument('--cidr',
                        metavar='CIDR',
                        type=validate_ip_cidr,
                        required=True if s in ('add', 'remove') else False,
                        default=None,
                        help='The IP CIDR to block')
        sp.add_argument('--vpc-name',
                        nargs='?',
                        default='azul-gitlab',
                        help='If specified, this ACL will be used instead of the GitLab VPC one')
        sp.set_defaults(func=actions[s])

    args = p.parse_args(sys.argv[1:])
    args.func(args.vpc_name, args.cidr)
