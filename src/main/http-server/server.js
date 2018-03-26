var fs = require('fs'); 
var express = require("express");

var file = "book.txt";

var streamReader = function(request, response){
    response.setHeader("Content-Type", "text/html");
	
	var stream = fs.createReadStream(file);
	stream.on("error", function(error) {
		response.writeHead(404, error);
		console.log("[" + new Date() + "] ERROR -- " + error);
		response.end();
	});
	console.log("[" + new Date() + "] INFO -- Request book.txt by Streaming");
    stream.pipe(response);
	
};

var wholeFileReader = function(request, response) {
	response.setHeader("Content-Type", "text/html");
	
	try {  
		var data = fs.readFileSync(file);
		console.log("[" + new Date() + "] INFO -- Request book.txt by Read whole file");
		response.write(data);
	} catch(error) {
		response.writeHead(404, error);
		console.log("[" + new Date() + "] ERROR -- " + error);
	} finally {
		response.end();
	}
};

var delay = 5000;

var slowReading = function(handler) {
	return function(request, response) {
		console.log("[" + new Date() + "] INFO -- Slow request");
		setTimeout(handler, delay, request, response);
	}
}

var app = express()

app.get("/", function (request, response) {
	response.setHeader("Content-Type", "text/html");
	response.send("Hello, world!");
});

app.get("/stream", streamReader);
app.get("/whole", wholeFileReader);
app.get("/slow-stream", slowReading(streamReader));
app.get("/slow-whole", slowReading(wholeFileReader));

app.listen(9999);
console.log("[" + new Date() + "] INFO -- Server is working");