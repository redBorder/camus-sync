require 'fileutils'
require 'webhdfs'
require 'webhdfs/fileutils'
require 'camus-sync/time_ext'

module CamusSync
  class Slot
    attr_reader :hdfs, :topic, :time, :paths, :pattern, :events

    def initialize(camus_path, hdfs, topic, time)
      @camus_path = camus_path
      @hdfs = hdfs
      @topic = topic
      @time = time
      @paths = paths!
      @pattern = pattern!
      @events = @paths.map do |path|
        File.basename(path).split('.')[3].to_i
      end.reduce(:+) || 0
    end

    def pattern!
      return '' if @paths.empty?
      path = @paths.first
      tokens = path.split('/')
      suffix = tokens[-1].split('.')
      tokens[-1] = "*.#{suffix[-1]}"
      tokens.join('/')
    end

    def paths!
      path = "#{@camus_path}/#{@topic}/hourly/#{@time.strftime("%Y/%m/%d/%H")}"
      begin
        @hdfs.list(path).map do |entry|
          File.join(path, entry['pathSuffix']) if entry['pathSuffix'] =~ /\.gz$/
        end
      rescue => e
        $log.warn("No events in #{path} at #{@hdfs.host}, ignoring")
        []
      end
    end

    def download
      @paths.each do |path|
        file = path.split('/')[-1]
        WebHDFS::FileUtils.copy_to_local(path, "#{LOCAL_TMP_DIR}/#{file}")
      end
    end

    def destroy
      $log.info("|-- deleting data", at: @hdfs.host)

      @paths.each do |path|
        @hdfs.delete(path)
      end
    end

    def upload(files)
      folder = @pattern.split('/')[0..-2].join('/')
      WebHDFS::FileUtils.set_server(@hdfs.host, 50070)

      files.each do |file|
        $log.info("|-- copying", file: file, folder: folder)
        WebHDFS::FileUtils.copy_from_local("#{LOCAL_TMP_DIR}/#{file}", "#{folder}/#{file}")
      end
    end
  end
end
