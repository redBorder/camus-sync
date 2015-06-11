require 'fileutils'
require 'webhdfs'
require 'webhdfs/fileutils'
require 'camus-sync/time_ext'
require 'camus-sync/slot_options'

module CamusSync
  class HDFS
    def initialize(camus_path, namenodes)
      @camus_path = camus_path
      @hdfs = []

      [namenodes].flatten.each do |host|
        begin
          $log.info("connecting to", namenode: host)
          hdfs = WebHDFS::Client.new(host, 50070)
          hdfs.list('/')
          @hdfs << hdfs
        rescue
          $log.info("failed to use", namenode: host)
        end
      end
      raise "no namenode is up and running" if @hdfs.empty?
    end

    def slots_options!(topic, interval)
      $log.info("scanning HDFS options for", interval: interval)
      enumerable_interval(interval).map do |time|
        SlotOptions.new(@camus_path, @hdfs, topic, Time.at(time).utc)
      end.reject do |slot|
        slot.best_slot.events.to_i < 1
      end
    end

    def enumerable_interval(interval)
      interval = interval.map { |t| t.floor(1.hour).utc }
      interval = (interval.first.to_i..interval.last.to_i)
      interval.step(1.hour)
    end
  end
end
