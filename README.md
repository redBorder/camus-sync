# Camus Sync

Synchronices hdfs data from camus between hdfs clusters.

## Usage

```
$ bin/camus-sync
You must supply -s PATH!
Usage: bin/camus-sync (options)
    -w, --window HOURS               scan window in hours, defaults to 24 hours
    -f, --offset HOURS               offset from now used as interval end, defaults to 3 hours
    -t, --topics LIST                Topics to process (comma seperated), defaults to all in sources.json
    -n, --namenodes LIST             HDFS namenodes (comma seperated), defaults to "localhost"
    -c, --camus-path PATH            HDFS path where camus saves its data
    -N, --dryrun                     do not submit tasks to overlord (dry-run)
    -h, --help                       Show this message
```

The repo contains examples for database.json and sources.json.

## Assumption / Notes

* HDFS contains data in gzip'd files in [camus](https://github.com/linkedin/camus)-style [folders](https://github.com/liquidm/druid-dumbo/blob/master/lib/dumbo/firehose/hdfs.rb#L65)

## Contributing

1. [Fork it](https://github.com/redborder/camus-sync/fork)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Credit

Based on [liquidm/druid-dumbo](https://github.com/redborder/druid-dumbo).
Dumbo lets you index camus data from HDFS to a [druid](http://www.druid.io) cluster
