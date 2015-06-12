REGISTER '/root/sync-hdfs-0.0.1-SNAPSHOT-selfcontained.jar';

RAW_DATA = LOAD '$RAW_DATA_FILES' USING net.redborder.synchdfs.RbSyncLoader('timestamp','src') AS (timestamp:chararray, src:chararray, data:Map[], count:int);
GROUP_DATA = GROUP RAW_DATA BY (src, timestamp);
DEDUPLICATE_DATA = FOREACH GROUP_DATA {
	  ORDER_DATA = ORDER RAW_DATA BY count DESC;
      JSON_DATA = LIMIT ORDER_DATA 1;
      GENERATE FLATTEN(JSON_DATA.data) AS (raw:Map[]);
};

AUX_VAR = GROUP DEDUPLICATE_DATA all;
COUNT = FOREACH AUX_VAR GENERATE COUNT(DEDUPLICATE_DATA);

STORE COUNT INTO 'hdfs://hadoopnamenode.redborder.cluster:8020/user/root/camus-sync/$JOB_ID/count';
STORE DEDUPLICATE_DATA INTO 'hdfs://hadoopnamenode.redborder.cluster:8020/user/root/camus-sync/$JOB_ID/data' USING net.redborder.synchdfs.RbSyncStorage();

