require File.dirname(__FILE__) + '/base_object'

class Redis
  #
  # Class representing a sorted set.
  #
  class StreamReader < BaseObject
    require 'redis/helpers/core_commands'
    include Redis::Helpers::CoreCommands

    attr_reader :options

    def initialize(*args)
      @options = args.last.is_a?(Hash) ? args.pop : {}
      @myredis = Objects::ConnectionPoolProxy.proxy_if_needed(args.first)
    end

    def read(id_or_ids_hash=nil, count: nil)
      result = perform_xread(id_or_ids_hash, count)
      parse_xread_result(result)
    end

    def next

    end

    private

    def perform_xread(id_or_ids_hash, count)
      id_or_ids_hash ||= "$"
      xread_args = ["BLOCK"]
      xread_args << options[:block] || 1
      xread_args += ["COUNT", count] if count
      xread_args << "STREAMS"
      xread_args += stream_keys
      xread_args += if id_or_ids_hash.is_a?(Hash)
                      stream_keys.map do |stream_key|
                        id_or_ids_hash[stream_key] || "$"
                      end
                    else
                      stream_keys.map { id_or_ids_hash }
                    end
      redis.xread(*xread_args)
    end

    def parse_xread_result(result)
      if single_stream?
        parse_members_arrays(result[0][1])
      else
        result.reduce({}) do |memo, (stream_key, members_arrays)|
          memo.merge!(stream_key => parse_members_arrays(members_arrays))
        end
      end
    end

    def parse_members_arrays(members_arrays)
      members_arrays.reduce({}) do |memo, (member_id, member_array)|
        memo.merge!(member_id => parse_member_array(member_array))
      end
    end

    def parse_member_array(member_array)
      member_array.each_slice(2).reduce({}) do |memo, (key, value)|
        memo.merge!(key => value)
      end
    end

    def stream_keys
      @stream_keys ||= begin
        Array(options[:streams] || options[:stream]).map do |stream_or_key|
          if stream_or_key.is_a?(String)
            stream_or_key
          else
            stream_or_key.key
          end
        end
      end
    end

    def single_stream?
      stream_keys.size == 1
    end
  end
end
