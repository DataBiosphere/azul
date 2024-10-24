function handler(event) {
    var response = event.response;
    var headers = response.headers;
    var request = event.request;
    var uri = request.uri;

    // Set HTTP security headers
    // Since JavaScript doesn't allow for hyphens in variable names, we use the dict["key"] notation 

    // Set cache control header …
    if (uri.startsWith('/_next/static/')) {
        // … for files in _next/static (versioned assets like JS, CSS)
        headers['cache-control'] = { value: 'public, max-age=31536000, immutable' };
    } else if (uri.startsWith('/api/')) {
        // … for API routes
        headers['cache-control'] = { value: 'no-store' };
    } else if (
        uri.startsWith('/static/')
        || uri.match(/\.(jpg|jpeg|png|svg|webp|gif|css|js)$/)
    ) {
        // … for images and other static assets
        headers['cache-control'] = { value: 'public, max-age=86400' };
    } else {
        // … for HTML pages and all other content
        headers['cache-control'] = { value: 'public, max-age=0, must-revalidate' };
    }

    // Return the response to viewers
    return response;
}