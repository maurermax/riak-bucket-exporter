
var program = require('commander');

program
    .version('0.0.1')
    .usage('[options] bucketName')
    .option('-H, --host [host]','specify the host (default: localhost)')
    .option('-p, --port [port]','specify the post (default: 8098)')
    .option('-f, --file [FileName]','specify the file name (default: [bucket].txt)')
    .parse(process.argv);
if(!program.args.length) {
    program.help();
}
var bucket = program.args;
program.host = program.host || 'localhost';
program.port = program.port || '8089';
program.file = program.file || bucket+'.txt';
var db = require("riak-js").getClient({host: "localhost", port: "8098"});
var fs = require('fs');
if (fs.existsSync(program.file)) {
  throw new Error('the output file already exists');
}
db.keys(bucket,{keys:'stream'}, end).on('keys', handleKey).start();

function end() {
  console.log('finished export to '+program.file);
}

function handleKey(keys) {
  for (var i=0;i<keys.length;i++) {
    var key = keys[i];
    processKey(key);
  }
}

function processKey(key) {
  db.get(bucket,key, function(err, obj, meta) {
    var out = '====== Key: '+key+' ======\n';
    out += JSON.stringify(obj, null, '\t')+'\n';
    fs.appendFile(program.file, out);
  });
}





