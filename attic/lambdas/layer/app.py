from azul import (
    config,
)
from azul.chalice import (
    AzulChaliceApp,
)

# This whole file only exists so that we can use Chalice to create the layer
# package and is removed from the final result.

app = AzulChaliceApp(app_name=config.qualified_resource_name('dependencies'),
                     globals=globals(),
                     spec={})


@app.route('/', spec={})
def foo():
    pass
