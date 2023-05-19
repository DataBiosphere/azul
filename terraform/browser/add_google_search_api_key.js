function handler(event) {
    
    var response400 = {
        statusCode: 400,
        statusDescription: 'Bad Request'
    };
    
    var searchEngineIds = {
            "anvil":"dac3c914beb0adbe9",
            "ncpi":"c83b7333b91318572"
        };
    
    var request = event.request;
    
    // cx parameter needs to be provided
    if(!request.querystring.cx){
        // no cx provided
        // block the request
        return response400;
    }

    //console.log(request.querystring["cx"].value);
    
    // Force cx value so this API can only be used to query the AnVIL or NCPI Google search engine and not others
    var searchEngineId = searchEngineIds[request.querystring.cx.value];
    
    if(!searchEngineId){
         // cx parameter was not one of the configured ones.
         // block the request
         return response400;
    }

    // We have a valid searchEngineId, replace cx.    
    request.querystring["cx"]={"value": searchEngineId};
    

    // Add the API key and let the request proceed
    request.querystring["key"]={"value": "${google_apikeys_key.google_search.key_string}"};
    
    return request;
}
