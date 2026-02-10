from azul.service.source_service import (
    SourceService,
)
from azul.template import (
    emit,
)

emit(SourceService().configured_sources_for_outsourcing)
