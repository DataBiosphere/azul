from contextlib import (
    ContextDecorator,
)
from logging import (
    getLogger,
)
import os

log = getLogger(__name__)


class RemoteDebugSession(ContextDecorator):
    """
    Adapted from https://stackoverflow.com/a/60333250/7830612

    According to the Stackoverflow post, since the debug session is over a
    long-running TCP connection, we need to close the connection explicitly
    within the lambda, otherwise the lambda will time out. A context manager
    handles this conveniently.

    How to use:

    1. In PyCharm select ``Run > Edit Configuration``, then The **+** to add
       a new **Python Debug Server**.

       Enter your IP address under **IDE host name**. If you don't have a
       static IP, leave the **host name** as ``localhost`` and install
       https://ngrok.com/. We will use Ngrok to tunnel a URL to the debug
       server on your machine.

       Enter a **Port**, such as ``8000``.

       Add the following **Path mappings**:

       +---------------------------------------+-----------------------------+
       | local path                            | remote path                 |
       +=======================================+=============================+
       | ``<absolute project root>/src/azul``  | ``/var/task/azul``          |
       +---------------------------------------+-----------------------------+
       | ``<absolute project root>/.venv/      | ``/var/task/chalice``       |
       | lib/python3.8/site-packages/chalice`` |                             |
       +---------------------------------------+-----------------------------+
       | ``<absolute project root              | ``/opt/python``             |
       | >/.venv/lib/python3.8/site-packages`` |                             |
       +---------------------------------------+-----------------------------+
       | ``<home directory>/                   | ``/var/lang/lib/python3.8`` |
       | .pyenv/versions/3.8.3/lib/python3.8`` |                             |
       +---------------------------------------+-----------------------------+

       Depending on which lambda you're debugging, add **one** of the
       following:

       =============================================== ====================
       local path                                      remote path
       =============================================== ====================
       ``<absolute path>/azul/lambdas/service/app.py`` ``/var/task/app.py``
       ``<absolute path>/azul/lambdas/indexer/app.py`` ``/var/task/app.py``
       =============================================== ====================

       Copy the ``pydevd-pycharm`` version listed in the configurations.

    2. Next make some changes.

       Add ``pydevd-pycharm==<version from previous step>`` to
       ``requirements.txt``.

       If using Ngrok run:

       ::

          ngrok tcp 8000

       and in your deployment's ``environment.py`` set
       ``AZUL_REMOTE_DEBUG_ENDPOINT`` to the Forwarding URL and port listed
       by Ngrok. Otherwise set these variables to the values you used in
       your configurations.

    3. Start the debug server, by clicking the debug icon with the
       configuration selected.

    4. Activate the remote debugger in one of two ways:

       -  Decorate the route you wish to debug with
          ``RemoteDebugSession()``. Make sure the ``RemoteDebugSession()``
          decorator is applied first:

          ::

             @app.route(...)
             @RemoteDebugSession()
             def my_route(...):
                 ...

       -  Use ``RemoteDebugSession()`` as a context manager around the code
          you wish to debug.

    5. Deploy:

       ::

          make package
          make deploy

    6. Set breakpoints, trigger the lambda, and debug!

    Warnings / caveats:
    ~~~~~~~~~~~~~~~~~~~

    -  DO NOT USE IN PRODUCTION. There are some security concerns with this
       process. Ngrok can potentially snoop on any data sent through its
       service. Secondly, a malicious party could connect to your debugger
       instead of the lambda and potentially extract data or crash your
       system. Also setting the wrong IP and port could also allow a
       malicious party to take control of the lambda.

    -  If the debug server isn't running lambdas will hang until they time
       out. Make sure you redeploy untainted code when done.

    -  By default our lambdas timeout after 30 seconds, which can make
       debugging difficult. For lambdas triggered by API Gateway, this is
       unavoidable. For other lambdas, you can increase the timeout manually
       by increasing ``lambda_timeout`` in ``config.json.template.py``.

    -  If multiple lambda instances try to connect to the debug server,
       latecomers will block and may eventually time out. One way to prevent
       this is to manually set the lambda's ``reserved_concurrency`` to 1 in
       ``config.json.template.py``.
    """

    def __init__(self):
        try:
            endpoint = os.environ['AZUL_REMOTE_DEBUG_ENDPOINT']
        except KeyError:
            log.error('Set AZUL_REMOTE_DEBUG_ENDPOINT to use remote debugging')
            raise
        else:
            self.host, port = endpoint.split(':')
            self.port = int(port)
        try:
            import pydevd
        except ImportError:
            log.error('Add correct version of pydevd-pycharm to requirements '
                      'to use remote debugging')
            raise
        else:
            self.pydevd = pydevd
        self.active = False

    def __enter__(self):
        self.pydevd.settrace(self.host,
                             port=self.port,
                             suspend=False,
                             stdoutToServer=True,
                             stderrToServer=True)
        log.info('Starting remote debugging session')
        self.active = True

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.active:
            log.info(f'Stopping remote debugging on {self.host}:{self.port}')
            self.pydevd.stoptrace()
            self.active = False
        return False
