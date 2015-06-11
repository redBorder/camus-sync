require 'multi_json'
require 'camus-sync/hdfs'

module CamusSync
  LOCAL_TMP_DIR = '/tmp/camus-sync'
    
  class CLI
    def initialize
      $log.info("scan", window: opts[:window])
      @topics = opts[:topics]
      @namenodes = opts[:namenodes]
      @camus_path = opts[:camus_path]
      $log.error('no topics specified') and exit if @topics.empty?
      @hdfs = HDFS.new(@camus_path, @namenodes)
      @interval = [((Time.now.utc-(opts[:window] + opts[:offset]).hours)).utc, (Time.now.utc-opts[:offset].hour).utc]
    end

    def run
      @topics.each do |topic|
        synchronize(topic)
      end
    end

    def synchronize(topic)
      $log.info("synchronizing hdfs data for", topic: topic)

      @hdfs.slots_options!(topic, @interval).each do |slot_options|
        slot_options.synchronize! opts[:dryrun]
      end
    end
  end
end
