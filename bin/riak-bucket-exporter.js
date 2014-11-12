#!/usr/bin/env node

var program = require('commander');

program
    .version('0.0.5')
    .usage('[options] bucketName')
    .option('-H, --host [host]','specify the host (default: localhost)')
    .option('-p, --port [port]','specify the post (default: 8098)')
    .option('-f, --file [FileName]','specify the file name (default: [bucket].json)')
    .parse(process.argv);
if(!program.args.length) {
    program.help();
}
var bucket = program.args;
program.host = program.host || 'localhost';
program.port = program.port || '8098';
program.file = program.file || bucket+'.json';
var count = 0;
var openWrites = 0;
var db = require("riak-js").getClient({host: program.host, port: program.port});
var fs = require('fs');
if (fs.existsSync(program.file)) {
  throw new Error('the output file already exists');
}
console.log('fetching bucket '+bucket+' from '+program.host+':'+program.port);
fs.appendFile(program.file, '[');
db.keys(bucket,{keys:'stream'}, function (err) {
  if (err) {
    console.log('failed to fetch keys');
    console.log(err);
  }
}).on('keys', handleKey).on('end', end).start();

function end() {
  if (openWrites>0) {
    setTimeout(end, 1000);
    return;
  }
  fs.appendFile(program.file, ']');
  if (count<=0) {
    console.log('nothing exported');
  } else {
    console.log('\nfinished export of '+count+' keys to '+program.file);
  }
}

function handleKey(keys) {
  if (count===0) {
    process.stdout.write("exporting");
  }
  count+=keys.length;
  for (var i=0;i<keys.length;i++) {
    var key = keys[i];
    openWrites++;
    processKey(key);
  }
}

var first = true;
function processKey(key) {
  db.get(bucket,key, function(err, obj, meta) {
    var out = {key: key};
    out.indexes = extractIndexes(meta);
    out.data = obj;
    if (!first) {
      fs.appendFile(program.file, ',');
    }
    fs.appendFile(program.file, JSON.stringify(out,null,'\t'));
    first=false;
    process.stdout.write(".");
    openWrites--;
  });
}

function extractIndexes(meta) {
  var indexes = {};
  var regex = /^x-riak-index-(.*)$/;
  for (var key in meta.headers) {
    var matches = key.match(regex);
    if (matches) {
      indexes[matches[1]] = meta.headers[key];
    }
  }
  return indexes;
}





