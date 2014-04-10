require "rubygems"
require "bundler/setup"
require "aws-sdk"
require 'pp'
require "httparty"


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
        if !(o.key =~ /.*[sign]/)
           o.copy_to(o.key,{:bucket => stage_bucket})
           options = {
               :body => {
                   :filename => o.key,
               }
           }
           begin
            HTTParty.post("http://localhost:#{port}/file_upload", options)
            o.delete
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

