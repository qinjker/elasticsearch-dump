require 'elasticsearch'
require 'optparse'
require 'optparse/time'
require 'ostruct'
require 'uri'

Version = [0,0,1]
class ESdumpOptions
    TYPE = ["data", "mapping"]
    def self.parse(args)
        options = OpenStruct.new
        options.input = ""
        options.output = ""
        options.type = ""

        opt_parser = OptionParser.new do |opts|
            opts.banner = "Usage: esdump.rb [options]"
            opts.separator ""
            opts.separator "Specific options:"

            opts.on("-i", "--input SOURCE",
                    "SOURCE could be an elasticsearch url or a file with .json extension") do |i|
                options.input = i
            end

            opts.on("-o", "--output TARGET",
                    "TARGET could be an elasticsearch url or a file with .json extension") do |o|
                options.output = o
            end

            opts.on("-t", "--type TYPE",
                    "TYPE must be one of [mapping, data]") do |t|
                abort "TYPE must be one of #{TYPE.inspect}" if !TYPE.include? t.downcase  
                options.type = t.downcase
            end

            opts.separator ""
            opts.separator "Common options:"
            
            opts.on("-h", "--help", "Show this message") do 
                puts opts
                exit
            end
            
            opts.on("-v", "--version", "Show version") do 
                puts ::Version.join('.')
            end
        end
        opt_parser.parse!(args)
        options
    end
end

class Util
end

class ESdump 
    def self.dump(input, output, type)
        case type
        when 'data'
            self.dump_data(input, output)
        when 'mapping'
            self.dump_mapping(input, output)
        else
            puts "wrong type: #{type}"
        end
    end
    def self.dump_data(input, output)      
        input_es = (input =~ URI::regexp)
        output_es = (output =~ URI::regexp)
        if input_es and not output_es
            self.dump_data_es2file(input, output)
        elsif input_es and output_es
            self.dump_data_es2es(input, output)
        end
    end
    def self.dump_data_es2file(input, output)
        index = input.split('/')[-1] 
        host = input[0, input.rindex('/')]
        cli = Elasticsearch::Client.new host: host
        r = cli.search index: index, scroll: '5m', size: 10
        File.open(output, "a", 0644) { |f|
            while r = cli.scroll(scroll_id: r['_scroll_id'], scroll: '5m') and not r['hits']['hits'].empty? do
                r['hits']['hits'].each do |doc|
                    f.write("#{doc.to_json}\n")
                end
            end
        }
    end
    def self.dump_data_es2es(input ,output)
        raise "not implemented"
    end

    def self.dump_mapping(input, output)
        input_es = (input =~ URI::regexp)
        abort "input[#{input}] must be is a valid url when dumping mapping" if !input_es 
        output_es = (output =~ URI::regexp)
        if input_es and not output_es
            self.dump_mapping_es2file(input, output)
        elsif input_es and output_es
            self.dump_mapping_es2es(input, output)
        end
    end
    def self.dump_mapping_es2file(input, output)
        index = input.split('/')[-1] 
        host = input[0, input.rindex('/')]
        cli = Elasticsearch::Client.new host: host
        File.open(output, "w+", 0644) { |f|
            mapping = cli.indices.get_mapping index: index
            f.write("#{mapping}\n")
        }
    end
    def self.dump_mapping_es2es(input, output)
        raise "not implemented"
    end
end

opts = ESdumpOptions.parse(ARGV)
ESdump.dump(opts.input, opts.output, opts.type)
