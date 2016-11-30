# riak-bucket-exporter #

Small command line utility to export bucket contents to a file.

### How to use ###

* `npm install -g riak-bucket-exporter`
* run `riak-bucket-exporter [bucketName]`
* `grab a cup of coffee`
* `export will be in bucketName.json`

### Any options? ###
````
Usage: riak-bucket-exporter [options] bucketName

  Options:

    -h, --help             output usage information
    -V, --version          output the version number
    -H, --host [host]      specify the host (default: localhost)
    -p, --port [port]      specify the post (default: 8098)
    -f, --file [FileName]  specify the file name (default: [bucket].json)
    -i, --import           import mode (instead of reading from bucket entries will be written to bucket)
    -q, --queue            specify the concurrency (default: 20)
    -m, --meta             save meta if necessary (default: false)
    -P, --pretty           stringify with tabs (default: false)
    --delete               delete the keys as they are exported (DANGER: possible data loss)
````
