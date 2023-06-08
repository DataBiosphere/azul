// Modify explore path based on host header

function handler(event) {

 var request = event.request;
 var host = request.headers['host'].value;

// FIXME: Stop hard-coding this
//        https://github.com/DataBiosphere/azul/issues/5151

 var hostPath = {
     "ux-dev.singlecell.gi.ucsc.edu":"ux-dev",
     "hca.dev.singlecell.gi.ucsc.edu":"hca",
     "lungmap.dev.singlecell.gi.ucsc.edu":"lungmap",
     "anvil.dev.singlecell.gi.ucsc.edu":"anvil",
     "anvil.gi.ucsc.edu":"anvil-cmg",
     "anvil-catalog.dev.singlecell.gi.ucsc.edu":"anvil-catalog",
     "ncpi-catalog.dev.singlecell.gi.ucsc.edu":"ncpi-catalog",
     "ncpi-catalog-dug.dev.singlecell.gi.ucsc.edu":"ncpi-catalog-dug",
 };

 // Default to ux-dev if site is unknown
 var path = hostPath[host];
 if (!path){
     path = "anvil-cmg";
 }

 var explorePath= "/explore/"+ path;
 var uri = request.uri;
 uri = uri.replace("/explore", explorePath);
 request.uri = uri;

  if(uri.endsWith(explorePath)){
     //this was a request for /explore, add the trailing slash.
     request.uri +="/index.html";
     return request;
 }

 if(uri.endsWith(explorePath+"/")){
     //this was a request for /explore/ will find its index.html
     request.uri +="index.html";
     return request;
 }

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
