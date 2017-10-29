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
      result = perform_xrange(*sanitize_xrange_args(args))
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
      redis.xadd key, timestamp, *member_to_array(member)
    end

    def perform_xrange(min, max, count)
      xrange_args = [key, min, max]
      xrange_args += ["COUNT", count] if count
      redis.xrange *xrange_args
    end

    def sanitize_xrange_args(args)
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
