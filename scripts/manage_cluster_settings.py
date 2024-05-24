from azul import (
    require,
)
from azul.es import (
    ESClientFactory,
)
from azul.logging import (
    configure_script_logging,
)

configure_script_logging()


def main():
    es = ESClientFactory.get()
    response = es.cluster.put_settings(body={
        'persistent': {
            'action.auto_create_index': False
        }
    })
    require(response['acknowledged'], 'Failed to update cluster settings', response)


if __name__ == '__main__':
    main()
