// Adds a trailing slash to the url to avoid 302 redirects

function handler(event) {
  
 var request = event.request;
 var uri = request.uri;
 
 if (uri == "/"){
     return request;
 }
 
 if(uri.endsWith("/")){
     //already ends with / so do not bother
     request.uri +="index.html";
     return request;
 }
 
  if(uri.includes(".")){
     // is a request for a file , leaeve alone
     return request;
 }
 
 // Otherwise add the trailing /
 request.uri +="/index.html";
   
 return request;
}