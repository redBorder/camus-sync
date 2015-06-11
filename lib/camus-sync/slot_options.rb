require 'fileutils'
require 'webhdfs'
require 'webhdfs/fileutils'
require 'camus-sync/slot'
require 'camus-sync/time_ext'

module CamusSync
  class SlotOptions
    attr_reader :topic, :time

    def initialize(camus_path, hdfs_servers, topic, time)
      @camus_path = camus_path
      @hdfs_servers = hdfs_servers
      @topic = topic
      @time = time
      @all = all!
    end

    def all!
      @hdfs_servers.map do |hdfs|
        Slot.new(@camus_path, hdfs, @topic, @time)
      end
    end

    def best_slot
      @all.max_by(&:events)
    end

    def synchronize!(dryrun = true)
      $log.info("synchronizing slot options at", time: @time, topic: @topic)

      unless @hdfs_servers.size > 1
        $log.info("tried to sync but there are no enough hdfs servers")
        return
      end

      all_events = @all.map(&:events).uniq
      unless all_events.size > 1
        $log.info("slot already in sync", time: @time, source: @topic)
        return
      end

      unless dryrun
        $log.info("downloading best option", time: @time, from: best_slot.hdfs.host)
        WebHDFS::FileUtils.set_server(best_slot.hdfs.host, 50070) unless dryrun

        FileUtils.remove_entry_secure LOCAL_TMP_DIR, true
        FileUtils.mkdir LOCAL_TMP_DIR

        best_slot.download
      end

      @all.each do |option|
        next if option == best_slot || option.events == best_slot.events
        $log.info("found differences between options", delta: (best_slot.events - option.events), best: best_slot.events, current: option.events)
        next if dryrun
        
        $log.info("|-- synchronizing", from: best_slot.hdfs.host, to: option.hdfs.host)
        option.destroy
        option.upload best_slot.paths.map { |p| p.split('/')[-1] }
        $log.info("|-- sync done", at: option.hdfs.host)
      end
    end 
  end
end
