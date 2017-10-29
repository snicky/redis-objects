require File.dirname(__FILE__) + '/base_object'

class Redis
  #
  # Class representing a sorted set.
  #
  class Stream < BaseObject
    require 'redis/helpers/core_commands'
    include Redis::Helpers::CoreCommands

    attr_reader :key, :options

    def <<(member)
      perform_xadd(member)
    end

    def []=(timestamp, member)
      perform_xadd(member, timestamp)
    end

    def range(*args)
      result = perform_xrange(args)
      result.reduce({}) do |memo, (member_id, member_array)|
        memo.merge!(member_id => parse_member_array(member_array))
      end
    end

    def members
      range
    end

    def length
      redis.xlen(key)
    end

    def empty?
      length == 0
    end

    private

    def perform_xadd(member, timestamp="*")
      xadd_args = [key]
      if (maxlength = options[:maxlength])
        xadd_args << "MAXLEN"
        xadd_args << "~" if options[:exact_length] == false
        xadd_args << maxlength
      end
      xadd_args += [timestamp, *member_to_array(member)]
      redis.xadd(*xadd_args)
    end

    def perform_xrange(args)
      min, max, count = sanitize_range_args(args)
      xrange_args = [key, min, max]
      xrange_args += ["COUNT", count] if count
      redis.xrange *xrange_args
    end

    def sanitize_range_args(args)
      if args[0].is_a?(Hash)
        limits = args[0]
        min = limits[:min]
        max = limits[:max]
        count = limits[:count]
      else
        min = args[0]
        max = args[1]
        count = args[2] && args[2][:count]
      end
      [ min || "-", max || "+", count]
    end

    def member_to_array(member)
      member.to_a.flatten(1)
    end

    def parse_member_array(member_array)
      member_array.each_slice(2).reduce({}) do |memo, (key, value)|
        memo.merge!(key => value)
      end
    end
  end
end
