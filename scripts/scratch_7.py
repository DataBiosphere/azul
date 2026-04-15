from azul import config
from azul.indexer.mirror_service import MirrorWorkerService
from azul.service import Filters
from azul.service.index_service import IndexService


def download(catalog, source_id, file_uuid):
    index_service = IndexService()
    mirror_service = MirrorWorkerService(catalog=catalog, schema_url_func=None)
    file = index_service.get_data_file(catalog=catalog,
                                       file_uuid=file_uuid,
                                       file_version=None,
                                       filters=Filters(explicit={},
                                                       source_ids={source_id}))
    assert file is not None

    data = mirror_service._download(file, part=None)
    print('Downloaded', len(data), 'bytes')


sandbox_args = {
    'source_id': 'b1083e8b-4de9-467a-97de-18179c4e6bd1',
    'file_uuid': '60e25442-aba0-4934-af42-be0d536112de'
}

hammerbox_args = {
    'source_id': 'b3b5fbcb-583d-4894-90bc-19abe85a0f4f',
    'file_uuid': '5a795c00-3df1-468d-b4a0-2e7fe048b6d4'
}


def main():
    deployment = config.deployment.name
    match deployment:
        case 'sandbox':
            args = sandbox_args
        case 'hammerbox':
            args = hammerbox_args
        case _:
            assert False, deployment

    download(
        catalog=config.default_catalog,
        **args
    )


if __name__ == '__main__':
    main()
