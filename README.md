# Camus Sync

Synchronices hdfs data from camus between hdfs clusters.

## Usage

```
$ java -cp "camus-sync-VERSION-selfcontained.jar:$(hadoop classpath)" net.redborder.camus.CamusSync -h
usage: java -cp CLASSPATH net.redborder.camus.CamusSync OPTIONS
 -c,--camus-path <arg>   HDFS path where camus saves its data
 -d,--dimensions-file <arg>   path to a YAML file that specifies an array
                              of dimensions for each topic that will be
                              used to identify duplicated events
 -f,--offset <arg>       offset
 -h,--help               print this help
 -m,--mode <arg>         task to execute (synchronize, deduplicate)
 -n,--namenodes <arg>    comma separated list of namenodes
 -N,--dry-run            do nothing
 -t,--topics <arg>       comma separated list of topics
 -w,--window <arg>       window hours
```

## Assumption / Notes

* HDFS contains data in gzip'd files in [camus](https://github.com/linkedin/camus)-style [folders](https://github.com/liquidm/druid-dumbo/blob/master/lib/dumbo/firehose/hdfs.rb#L65)
* The dimensions file is a YAML file with a map, where the keys are the topics, and the value for each key is an
array of strings with the dimensions that will be used to identify duplicated events

## Modes

### Synchronize

This mode does the following for each hour:
1. Finds out which namenode has the biggest number of events for that hour.
2. Deletes all the data from that hour from every other namenode.
3. Copies the data from (1) to every other namenode.

It uses the distCp hadoop job to copy the data directly between the clusters.

### Deduplicate

This mode runs a Pig job that loads the data from each namenode specified and merges all the data, deleting
duplicated rows along the way. We expect the data to be json messages without depth. You can specify which
dimensions will be used to identify which rows are duplicated with the `-d` option.

## Contributing

1. [Fork it](https://github.com/redborder/camus-sync/fork)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Credit

Based on [liquidm/druid-dumbo](https://github.com/redborder/druid-dumbo).
Dumbo lets you index camus data from HDFS to a [druid](http://www.druid.io) cluster
