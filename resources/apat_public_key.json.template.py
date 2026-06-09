from azul.service.user_service import (
    UserService,
)
from azul.template import (
    emit,
)

emit(UserService().apat_public_key_for_outsourcing)
