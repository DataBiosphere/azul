from azul.service.source_service import (
    SourceService,
)
from azul.template import (
    emit,
)

emit(SourceService().all_sources_for_outsourcing)
