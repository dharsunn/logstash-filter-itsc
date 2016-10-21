# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require "open3"
require "net/http"
require "json"
require "csv"

# This example filter will replace the contents of the default
# message field with whatever you specify in the configuration.
#
# It is only intended to be used as an example.
class LogStash::Filters::Itsc < LogStash::Filters::Base

  # Setting the config_name here is required. This is how you
  # configure this filter from your Logstash config.
  #
  # filter {
  #   example {
  #     message => "My message..."
  #   }
  # }
  #
  config_name "itsc"

  # Replace the message with this value.
  config :url, :validate => :string, :default => "ITSC"


  public
  def register
    # Add instance variables
  end # def register

  def write_data_to_csv(api_url,csv_file_name,key,value)

    # Connect with Url-ShortName API
    url = URI.parse(api_url)
    req = Net::HTTP::Get.new(url.to_s)
    res = Net::HTTP.start(url.host, url.port) {|http|
      http.request(req)}

    # Get JSON reponse from API
    response = res.body
    json_out = JSON.parse(String.try_convert(response))

    CSV.open(csv_file_name, "w") do |csv|
      csv << [key, value]
      for i in 0..json_out["resource"].size
        begin
          key_string = json_out["resource"][i][key]
          value_string = json_out["resource"][i][value]
          csv << [key_string, value_string]
        rescue Exception
        end
      end
    end
  end

  # Match request_url with csv file to get short names and store the short names in an array
  def match_url_to_short_name(url_to_short_file, url, event)
    short_name_output = Array.new
    count = 0;
    CSV.foreach(url_to_short_file) do |row|
      # If the url matches, add short names to the array
      if row[0].include? url and not short_name_output.include? row[1]
        short_name_output[count] = row[1]
        count = count + 1
        # Special Case : Few files are not available in the api but their directories are available
        # So those short names are included
      elsif url.include? "browse/"
          index = url.index("browse/") + 5
          temp_url = url[0..index]
          #puts temp_url
          if row[0].include? temp_url and not short_name_output.include? row[1]
            short_name_output[count] = row[1]
            count = count + 1
          end
      else
        index = url.rindex('/')
        temp_url = url[0..index]
        if row[0].include? temp_url and not short_name_output.include? row[1]
          short_name_output[count] = row[1]
          count = count + 1
        end
      end
    end

    return short_name_output
  end

  # Match short names with csv file to get dataset_names and store the dataset names in an array
  def match_short_name_to_dataset(short_to_dataset_file, short_name_output)
    dataset_output = Array.new
    count = 0;
    CSV.foreach(short_to_dataset_file) do |row|
      for i in 0..short_name_output.size
        if row[0].eql? short_name_output[i]
          dataset_output[count] = row[1]
          count = count + 1
        end
      end
    end

    return dataset_output
  end

  # Match short names with csv file to get dataset_names and store the dataset names in an array
  def match_short_name_to_collection(short_to_collection_file, short_name_output)
    collection_output = Array.new
    count = 0;
    CSV.foreach(short_to_collection_file) do |row|
      for i in 0..short_name_output.size
        if row[0].eql? short_name_output[i]
          collection_output[count] = row[1]
          count = count + 1
        end
      end
    end
    return collection_output
  end

  public
  def filter(event)

    if @url
      short_name_output = Array.new
      dataset_output = Array.new
      collection_output = Array.new

      request_url = event.get(@url)

      url_to_short_name_url = "http://ec2-54-201-117-192.us-west-2.compute.amazonaws.com/api/v2/ghrc_catalog_dev/_table/cm_idims.ds_urls?api_key=8736e7dca88416f8c818d57a1e65e0c8b96075b42f911354a32b14b7ef80d317"
      short_name_to_dataset_url = "http://ec2-54-201-117-192.us-west-2.compute.amazonaws.com/api/v2/ghrc_catalog_dev/_table/cm_idims.ds_info?filter=(local_visible=%27Y%27)&api_key=8736e7dca88416f8c818d57a1e65e0c8b96075b42f911354a32b14b7ef80d317"
      short_name_to_collection_url = "http://ec2-54-201-117-192.us-west-2.compute.amazonaws.com/api/v2/ghrc_catalog_dev/_table/cm_idims.ds_colls?api_key=8736e7dca88416f8c818d57a1e65e0c8b96075b42f911354a32b14b7ef80d317"

      # Interval to redownload JSON from API
      duration = 24*60*60

      # CSV filename for saving Url to Short Name JSON
      url_to_short_file = "Url_To_ShortName.csv"

      # Check if file exits or if the duration exceeded to download and write into else read from the csv file
      if File.exist?(url_to_short_file)
        file_time = File.mtime(url_to_short_file)
        now_time = Time.now

        if now_time.to_f - file_time.to_f > duration
          write_data_to_csv(url_to_short_name_url,url_to_short_file,"ds_url","ds_short_name")
          short_name_output = match_url_to_short_name(url_to_short_file, request_url, event)
        else
          short_name_output = match_url_to_short_name(url_to_short_file, request_url, event)
        end

      else
        write_data_to_csv(url_to_short_name_url,url_to_short_file,"ds_url","ds_short_name")
        short_name_output = match_url_to_short_name(url_to_short_file, request_url, event)
      end

      # CSV filename for saving ShortName To DatasetName JSON
      short_to_dataset_file = "Short_To_DatasetName.csv"

      # Check if file exits or if the duration exceeded to download and write into the file else read from the csv file
      if File.exist?(short_to_dataset_file)
        file_time = File.mtime(short_to_dataset_file)
        now_time = Time.now

        if now_time.to_f - file_time.to_f > duration
          write_data_to_csv(short_name_to_dataset_url,short_to_dataset_file,"ds_short_name","dataset_name")
          dataset_output = match_short_name_to_dataset(short_to_dataset_file, short_name_output)
        else
          dataset_output = match_short_name_to_dataset(short_to_dataset_file, short_name_output)
        end

      else
        write_data_to_csv(short_name_to_dataset_url,short_to_dataset_file,"ds_short_name","dataset_name")
        dataset_output = match_short_name_to_dataset(short_to_dataset_file, short_name_output)
      end

      # CSV filename for saving ShortName To DatasetName JSON
      short_to_collection_file = "Short_To_CollectionName.csv"

      # Check if file exits or if the duration exceeded to download and write into the file else read from the csv file
      if File.exist?(short_to_collection_file)
        file_time = File.mtime(short_to_collection_file)
        now_time = Time.now
        #puts file_time
        #puts now_time

        if now_time.to_f - file_time.to_f > duration
          write_data_to_csv(short_name_to_collection_url,short_to_collection_file,"ds_short_name","coll_name")
          collection_output = match_short_name_to_collection(short_to_collection_file, short_name_output)
        else
          collection_output = match_short_name_to_collection(short_to_collection_file, short_name_output)
        end

      else
        write_data_to_csv(short_name_to_collection_url,short_to_collection_file,"ds_short_name","coll_name")
        collection_output = match_short_name_to_collection(short_to_collection_file, short_name_output)
      end


      # Create a variable name in logstash to store the short_name_output and dataset_name_output
      #event.set("Url",request_url)
      event.set("ds_short_name",short_name_output)
      event.set("dataset_name",dataset_output)
      event.set("collection_name", collection_output)

      @logger.debug? && @logger.debug("Message is now: #{event.get("message")}")
    end

    # filter_matched should go in the last line of our successful code
    filter_matched(event)
  end # def filter
end # class LogStash::Filters::Itsc
