function handler(event) {
    var request = event.request;

    var new_uri = request.uri.replace(/^\/explore(\/|$)/,'/');
    if( request.uri !== new_uri ) {
        return {
            statusCode: 301,
            statusDescription: 'Moved Permanently',
            headers: {
                'location': {'value': new_uri}
            }
        }
    }

    // @alex explorePath needs to be configurable to the path of the behavior like
    // /explore, /data or /ncpi/data or ""
    var explorePath = ""

    var uri = request.uri;
    if (uri === "/") {
        // Default root
        uri = explorePath + "/index.html";
    }
    request.uri = uri;

    if (explorePath && uri.endsWith(explorePath)) {
        // this was a request for /explore, add the default root
        request.uri += "/index.html";
        return request;
    }

    if (explorePath && uri.endsWith(explorePath + "/")) {
        // this was a request for /explorePath/ add the default root
        request.uri += "index.html";
        return request;
    }

    if (uri.includes(".")) {
        // is a request for a file, leave alone
        return request;
    }

    if (uri.endsWith("/")) {
        //this was a request for for something like /explorePath/files/ remove the trailing /
        request.uri = request.uri.slice(0, -1);
    }

    // final case add  .html as this was not a file /explorePath or /explorePath/
    request.uri += ".html";

    return request;
}
