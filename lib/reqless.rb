# Encoding: utf-8

require 'socket'
require 'redis'
require 'json'
require 'securerandom'

module Reqless
  # Define our error base class before requiring the other files so they can
  # define subclasses.
  Error = Class.new(StandardError)
end

require 'reqless/version'
require 'reqless/config'
require 'reqless/queue'
require 'reqless/queue_priority_pattern'
require 'reqless/throttle'
require 'reqless/job'
require 'reqless/lua_script'
require 'reqless/failure_formatter'

module Reqless
  UnsupportedRedisVersionError = Class.new(Error)

  def generate_jid
    SecureRandom.uuid.gsub('-', '')
  end

  def stringify_hash_keys(hash)
    hash.each_with_object({}) do |(key, value), result|
      result[key.to_s] = value
    end
  end

  def failure_formatter
    @failure_formatter ||= FailureFormatter.new
  end

  module_function :generate_jid, :stringify_hash_keys, :failure_formatter

  # A class for interacting with jobs. Not meant to be instantiated directly,
  # it's accessed through Client#jobs
  class ClientJobs
    def initialize(client)
      @client = client
    end

    def complete(offset = 0, count = 25)
      JSON.parse(@client.call('jobs.completed', offset, count))
    end

    def tracked
      results = JSON.parse(@client.call('jobs.tracked'))
      results['jobs'] = results['jobs'].map { |j| Job.new(@client, j) }
      results
    end

    def tagged(tag, offset = 0, count = 25)
      results = JSON.parse(@client.call('jobs.tagged', tag, offset, count))
      # Should be an empty array instead of an empty hash
      results['jobs'] = [] if results['jobs'] == {}
      results
    end

    def failed(t = nil, start = 0, limit = 25)
      if !t
        return JSON.parse(@client.call('failureGroups.counts'))
      else
        results = JSON.parse(@client.call('jobs.failedByGroup', t, start, limit))
        results['jobs'] = multiget(*results['jobs'])
        results
      end
    end

    def [](id)
      get(id)
    end

    def get(jid)
      results = @client.call('job.get', jid)
      if results.nil?
        results = @client.call('recurringJob.get', jid)
        return nil if results.nil?
        return RecurringJob.new(@client, JSON.parse(results))
      end
      Job.new(@client, JSON.parse(results))
    end

    def multiget(*jids)
      results = JSON.parse(@client.call('job.getMulti', *jids))
      results.map do |data|
        Job.new(@client, data)
      end
    end
  end

  # A class for interacting with workers. Not meant to be instantiated
  # directly, it's accessed through Client#workers
  class ClientWorkers
    def initialize(client)
      @client = client
    end

    def counts
      JSON.parse(@client.call('workers.counts'))
    end

    def [](name)
      JSON.parse(@client.call('worker.jobs', name))
    end
  end

  # A class for interacting with queues. Not meant to be instantiated directly,
  # it's accessed through Client#queues
  class ClientQueues
    def initialize(client)
      @client = client
    end

    def counts
      JSON.parse(@client.call('queues.counts'))
    end

    def [](name)
      Queue.new(name, @client)
    end
  end

  # A class for interacting with throttles. Not meant to be instantiated directly,
  # it's accessed through Client#throttles
  class ClientThrottles
    def initialize(client)
      @client = client
    end

    def [](name)
      Throttle.new(name, @client)
    end

    def counts
      @client.queues.counts.map do |queue|
        Queue.new(queue['name'], @client).throttle
      end
    end
  end

  class ClientQueuePatterns
    def initialize(client)
      @client = client
    end

    def get_queue_identifier_patterns
      serialized_patterns = @client.call('queueIdentifierPatterns.getAll')
      identifiers_with_serialized_patterns = JSON.parse(serialized_patterns)
      Hash[
        identifiers_with_serialized_patterns.map do |identifier, serialized_pattern|
          [identifier, JSON.parse(serialized_pattern)]
        end
      ]
    end

    def set_queue_identifier_patterns(identifier_patterns)
      args = identifier_patterns.flat_map do |identifier, pattern|
        [identifier, JSON.dump(pattern)]
      end
      @client.call('queueIdentifierPatterns.setAll', *args)
    end

    def get_queue_priority_patterns
      serialized_priority_patterns_json = @client.call('queuePriorityPatterns.getAll')
      serialized_priority_patterns = JSON.parse(serialized_priority_patterns_json)
      serialized_priority_patterns.map do |serialized_priority_pattern|
        priority_pattern = JSON.parse(serialized_priority_pattern)
        QueuePriorityPattern.new(
          priority_pattern['pattern'],
          priority_pattern.fetch('fairly', false),
        )
      end
    end

    def set_queue_priority_patterns(queue_priority_patterns)
      serialized_patterns = queue_priority_patterns.map do |queue_priority_pattern|
        JSON.dump({
          'pattern': queue_priority_pattern.pattern,
          'fairly': queue_priority_pattern.should_distribute_fairly,
        })
      end
      @client.call('queuePriorityPatterns.setAll', *serialized_patterns)
    end
  end

  # A class for interacting with events. Not meant to be instantiated directly,
  # it's accessed through Client#events
  class ClientEvents
    EVENTS = %w{canceled completed failed popped stalled put track untrack}
    EVENTS.each do |method|
      define_method(method.to_sym) do |&block|
        @actions[method.to_sym] = block
      end
    end

    def initialize(redis)
      @redis   = redis
      @actions = {}
    end

    def listen
      yield(self) if block_given?
      channels = EVENTS.map { |event| "ql:#{event}" }
      @redis.subscribe(channels) do |on|
        on.message do |channel, message|
          callback = @actions[channel.sub('ql:', '').to_sym]
          callback.call(message) unless callback.nil?
        end
      end
    end

    def stop
      @redis.unsubscribe
    end
  end

  # The client for interacting with Reqless
  class Client
    attr_reader :_reqless, :config, :redis, :jobs, :queue_patterns, :queues, :throttles, :workers
    attr_accessor :worker_name

    def initialize(options = {})
      default_options = {:ensure_minimum_version => true}
      options = default_options.merge(options)

      should_ensure_minimum_redis_version = options.delete(:ensure_minimum_version)
      # This is the redis instance we're connected to. Use connect so REDIS_URL
      # will be honored
      @redis   = options[:redis] || Redis.new(options)
      @options = options
      assert_minimum_redis_version('2.5.5') if should_ensure_minimum_redis_version
      @config = Config.new(self)
      @_reqless = Reqless::LuaScript.new('reqless', @redis, :on_reload_callback => @options[:on_lua_script_reload_callback])

      @jobs    = ClientJobs.new(self)
      @queues  = ClientQueues.new(self)
      @throttles  = ClientThrottles.new(self)
      @queue_patterns = ClientQueuePatterns.new(self)
      @workers = ClientWorkers.new(self)
      @worker_name = [Socket.gethostname, Process.pid.to_s].join('-')
    end

    def inspect
      "<Reqless::Client #{@options} >"
    end

    def events
      # Events needs its own redis instance of the same configuration, because
      # once it's subscribed, we can only use pub-sub-like commands. This way,
      # we still have access to the client in the normal case
      @events ||= ClientEvents.new(Redis.new(@options))
    end

    def call(command, *argv)
      @_reqless.call(command, Time.now.to_f, *argv)
    end

    def track(jid)
      call('job.track', jid)
    end

    def untrack(jid)
      call('job.untrack', jid)
    end

    def tags(offset = 0, count = 100)
      JSON.parse(call('tags.top', offset, count))
    end

    def deregister_workers(*worker_names)
      call('worker.forget', *worker_names)
    end

    def bulk_cancel(jids)
      call('job.cancel', jids)
    end

    def new_redis_connection
      @redis.dup
    end

    def ==(other)
      self.class == other.class && redis.id == other.redis.id
    end
    alias eql? ==

    def hash
      self.class.hash ^ redis.id.hash
    end

  private

    def assert_minimum_redis_version(version)
      # remove the "-pre2" from "2.6.8-pre2"
      redis_version = @redis.info.fetch('redis_version').split('-').first
      return if Gem::Version.new(redis_version) >= Gem::Version.new(version)

      raise UnsupportedRedisVersionError,
            "Reqless requires #{version} or better, not #{redis_version}"
    end
  end
end
