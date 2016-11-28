#!/usr/bin/env node

var program = require('commander');
var async = require('async');

program
    .version('0.0.8')
    .usage('[options] bucketName')
    .option('-H, --host [host]','specify the host (default: localhost)')
    .option('-p, --port [port]','specify the post (default: 8098)')
    .option('-f, --file [FileName]','specify the file name (default: [bucket].json)')
    .option('-i, --import','import mode (instead of reading from bucket entries will be written to bucket)')
    .option('-c, --concurrency [concurrency]','specify the concurrency (default: 20)')
    .option('-m, --meta [meta]', 'import with meta (default: False)')
    .option('-P, --pretty [pretty]', 'pretty stringify of json (default: False)')
    .option('--delete', 'delete the keys as they are exported (DANGER: possible data loss)')
    .parse(process.argv);
if(!program.args.length) {
    program.help();
}
var bucket = program.args;
program.host = program.host || 'localhost';
program.port = program.port || '8098';
program.file = program.file || bucket+'.json';
program.concurrency = program.concurrency || 20;
program.meta = program.meta || false;
program.pretty = program.pretty || false;
var count = 0;
var openWrites = 0;
var db = require("riak-js").getClient({host: program.host, port: program.port});
var fs = require('fs');
var deleteKeys = !program.import && !!program.delete;

if (program.import) {
  importToBucket();
} else {
  if (program.delete) {
    console.log('WARNING: keys will be deleted as they are exported');
  }

  exportFromBucket();
}

function importToBucket() {
  if (!fs.existsSync(program.file)) {
    throw new Error('the import file does not exist');
  }
  fs.readFile(program.file, 'utf8', function (err,data) {
    if (err) {
      return console.log(err);
    }
    var entries = JSON.parse(data);
    async.eachLimit(entries, program.concurrency, function(entry, cb) {
      console.log('inserting entry with key %j', entry.key);
      var meta = {index: entry.indexes};
      if(entry.meta){
        meta = entry.meta;
      }
      db.save(bucket, entry.key, entry.data, meta, function(err) {
        if (err) {
          return cb(err);
        }
        cb(null);
      });
    }, function(err) {
      if (err) {
        return console.log(err);
      }
      return console.log('%j entries inserted into bucket %j', entries.length, bucket);
    });
  });
}

var receivedAll = false;
var q = async.queue(processKey, program.concurrency);
q.drain = end;

function exportFromBucket() {
  if (fs.existsSync(program.file)) {
    throw new Error('the output file already exists');
  }
  console.log('fetching bucket '+bucket+' from '+program.host+':'+program.port);
  fs.appendFileSync(program.file, '[');
  db.keys(bucket,{keys:'stream'}, function (err) {
    if (err) {
      console.log('failed to fetch keys');
      console.log(err);
    }
  }).on('keys', handleKeys).on('end', function() {
    console.log('received all keys');
    receivedAll = true;
  }).start();
}

function end() {
  if (!receivedAll) {
    return;
  }
  fs.appendFileSync(program.file, ']');
  if (count<=0) {
    console.log('nothing exported');
  } else {
    console.log('finished export of '+count+' keys to '+program.file);
  }
}

function handleKeys(keys) {
  count+=keys.length;
  for (var i=0;i<keys.length;i++) {
    var key = keys[i];
    openWrites++;
    q.push(key, function() {
      openWrites--;
    });
  }
  console.log('queue size: ' + q.length());
}

var first = true;
function processKey(key, cb) {
  console.log('exporting key ' + key);
  db.get(bucket,key, function(err, obj, meta) {
    var out = {key: key};
    out.indexes = extractIndexes(meta);
    out.data = obj;
    if(program.meta){
      out.meta = meta;
    }
    out.meta = meta;
    if (!first) {
      fs.appendFileSync(program.file, ',');
    }
    var options = [out];
    if(program.pretty){
      options = options.concat([null, '\t']);
    }
    fs.appendFileSync(program.file, JSON.stringify.apply(this, options));
    first=false;

    if (!deleteKeys) {
      return cb();
    }

    db.remove(bucket, key, function (err) {
      if(err){
        console.log(bucket, key, err)
      }
      cb();
    });
  });
}

function extractIndexes(meta) {
  var indexes = {};
  var regex = /^x-riak-index-(.*)_(.*)$/;
  if (meta != null) {
    for (var key in meta.headers) {
      var matches = key.match(regex);
      if (matches) {
        var name = matches[1];
        var type = matches[2];
        var val = meta.headers[key];
        if (type==='int') {
          val = parseInt(val, 10);
        }
        indexes[name] = val;
      }
    }
  }
  return indexes;
}





