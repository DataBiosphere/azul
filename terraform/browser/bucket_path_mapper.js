function handler(event) {
    var request = event.request;

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
