# Camus Sync

Synchronices hdfs data from camus between hdfs clusters.

## Usage

```
$ java -cp "camus-sync-VERSION-selfcontained.jar:$(hadoop classpath)" net.redborder.camus.CamusSync -h
usage: java -cp CLASSPATH net.redborder.camus.CamusSync OPTIONS
 -c,--configFile <arg>   path to a YAML config file
 -f,--offset <arg>       offset
 -h,--help               print this help
 -m,--mode <arg>         task to execute (synchronize, deduplicate)
 -n,--namenodes <arg>    comma separated list of namenodes
 -d,--dryRun             do nothing
 -p,--camusPath <arg>    HDFS path where camus saves its data
 -t,--topics <arg>       comma separated list of topics
 -w,--window <arg>       window hours
```

All the options can be specified in the config file specified with the -c option. Options from the
command line overwrite the options from the config file.

If the "topic" option is not specified on the command line, it will try to get the keys specified on the
property "topics" in the config file.

## Assumption / Notes

* HDFS contains data in gzip'd files in [camus](https://github.com/linkedin/camus)-style [folders](https://github.com/liquidm/druid-dumbo/blob/master/lib/dumbo/firehose/hdfs.rb#L65)
* The config file is a YAML file with a map, where the keys are the longer name of the option

## Deduplicate mode

This mode runs a Pig job that loads the data from each namenode specified and merges all the data, deleting
duplicated rows along the way. We expect the data to be json messages without depth.

To identify duplicated rows, you must specify a set of dimensions (properties) that will be used. If two or more
messages have the same value for each of these dimensions, all of those messages will be deleted, except one. Therefore,
you should use dimensions that can identify a message uniquely. We often include the timestamp of the message in this set
of dimensions.

You can specify the dimensions that will be used for each topic on the config file, under the key "topics".
The value of "topics" should be a map, where the key is the topic name, and the value is an array of strings, where
each string is the name of a dimension (property) on the JSON message.

## Synchronize mode

This mode does the following for each hour:
1. Finds out which namenode has the biggest number of events for that hour.
2. Deletes all the data from that hour from every other namenode.
3. Copies the data from (1) to every other namenode.

It uses the distCp hadoop job to copy the data directly between the clusters.

## Contributing

1. [Fork it](https://github.com/redborder/camus-sync/fork)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Credit

Based on [liquidm/druid-dumbo](https://github.com/redborder/druid-dumbo).
Dumbo lets you index camus data from HDFS to a [druid](http://www.druid.io) cluster
