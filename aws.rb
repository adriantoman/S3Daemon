require "rubygems"
require "bundler/setup"
require "aws-sdk"
require 'pp'
require "httparty"
require "csv"
require "pry"
require "tempfile"

@access_key = ARGV[0]
@secret_key = ARGV[1]
@port = ARGV[1]

class AwsDaemon

  def run(access_key,secret_key,port)
    s3 = AWS::S3.new(:access_key_id => access_key, :secret_access_key => secret_key)
    source_bucket = s3.buckets['gcl-data']
    stage_bucket = s3.buckets["gcl-data-stage"]
    logger = Logger.new("log.txt")

    while (true)

      source_bucket.objects.each do |o|
        key = o.key
        if !(key =~ /.*sign/)
           o.copy_to(key,{:bucket => stage_bucket})
           options = {
               :body => {
                   :filename => key,
               }
           }
           begin
            HTTParty.post("http://localhost:#{port}/file_upload", options)
            o.delete
            staged_o = stage_bucket.objects[key]
            file_headers = nil
            begin
              file = Tempfile.new("staged")
              staged_o.read do |chunk|
                file.write(chunk)
              end
              file.flush
              CSV.foreach(file.path, :return_headers => true, :headers => true) do |row|
                if row.header_row?
                  file_headers = row.headers
                else
                  break
                end
              end
            ensure
              file.close
            end
            logger.info "#{key} -- keys #{file_headers.join(", ")}"
            HTTParty.post("http://localhost:#{port}/file_columns", {
              :body => {
                :columns => file_headers.map {|name| {
                  :name => name
                }}
              }
            })
           rescue => e
            logger.error e.message
           end
        end

      end
      sleep(10)
    end


  end

end

temp = AwsDaemon.new
temp.run(ARGV[0],ARGV[1],ARGV[2])

