import json
import os
from http.server import (
    HTTPServer,
    SimpleHTTPRequestHandler,
)

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from azul.modules import load_app_module

spec_file = 'openapi.json'
parent_dir = os.path.realpath(os.path.dirname(__file__))


def write_specs(app, openapi_spec):
    specs = app.annotated_specs(openapi_spec)
    with open(os.path.join(parent_dir, spec_file), 'w') as f:
        json.dump(specs, f, indent=4)


def main():
    static_dir = os.path.join(parent_dir, 'apidev_static')
    web_page = os.path.join(static_dir, 'swagger-editor.html')

    host, port = 'localhost', 8787
    server_url = f"http://{host}:{port}"
    httpd = HTTPServer((host, port), SimpleHTTPRequestHandler)
    address = f"{server_url}/{os.path.relpath(web_page)}?url={server_url}/{os.path.relpath(parent_dir)}/{spec_file}"
    print(f'Open {address} in browser to validate changes.')
    print('If you changed/added any routes, make sure to deploy afterwards (also update API version)!')
    print('This is because the specs are pulled from API Gateway which needs '
          'to be up to date with any new routes')

    service = load_app_module('service')
    event_handler = UpdateHandler(service)
    observer = Observer()
    observer.schedule(event_handler, path=os.path.dirname(service.__file__), recursive=False)
    observer.start()

    write_specs(service.app, service.openapi_spec)
    httpd.serve_forever()


class UpdateHandler(FileSystemEventHandler):

    def __init__(self, service):
        self.service = service
        self.tracked_file = os.path.join(os.path.dirname(service.__file__), 'app.py')

    def on_modified(self, event):
        if event.src_path == self.tracked_file:
            self.service = load_app_module('service')
            write_specs(self.service.app, self.service.openapi_spec)
            print('Spec updated')


if __name__ == "__main__":
    main()
