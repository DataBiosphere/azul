function handler(event) {

 var request = event.request;

 var uri = request.uri;


 if(uri.includes(".")){
     // is a request for a file , leaeve alone
     return request;
 }

 if(uri.endsWith("/")){
    //this was a request for for something like /explore/files/ remove the trailing /
    request.uri = request.uri.slice(0, -1);
 }

 // final case add  .html as this was not a file /explore or /explore/
 request.uri +=".html";

 return request;
}
