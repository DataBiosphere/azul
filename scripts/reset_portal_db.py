"""
Clear all versions of the portal database from S3 and DynamoDB.
"""
from azul.portal_service import (
    PortalService,
)


def main():
    portal_service = PortalService()
    portal_service.overwrite(portal_service.default_db)


if __name__ == '__main__':
    main()
