# Encoding: utf-8

require 'reqless/job'
require 'redis'
require 'json'

module Reqless
  # A class for interacting with jobs in different states in a queue. Not meant
  # to be instantiated directly, it's accessed with Queue#jobs
  class QueueJobs
    def initialize(name, client)
      @name   = name
      @client = client
    end

    def running(start = 0, count = 25)
      JSON.parse(@client.call('queue.jobsByState', 'running', @name, start, count))
    end

    def throttled(start = 0, count = 25)
      JSON.parse(@client.call('queue.jobsByState', 'throttled', @name, start, count))
    end

    def stalled(start = 0, count = 25)
      JSON.parse(@client.call('queue.jobsByState', 'stalled', @name, start, count))
    end

    def scheduled(start = 0, count = 25)
      JSON.parse(@client.call('queue.jobsByState', 'scheduled', @name, start, count))
    end

    def depends(start = 0, count = 25)
      JSON.parse(@client.call('queue.jobsByState', 'depends', @name, start, count))
    end

    def recurring(start = 0, count = 25)
      JSON.parse(@client.call('queue.jobsByState', 'recurring', @name, start, count))
    end
  end

  # A class for interacting with a specific queue. Not meant to be instantiated
  # directly, it's accessed with Client#queues[...]
  class Queue
    attr_reader   :name, :client

    def initialize(name, client)
      @client = client
      @name   = name
    end

    # Our worker name is the same as our client's
    def worker_name
      @client.worker_name
    end

    def jobs
      @jobs ||= QueueJobs.new(@name, @client)
    end

    def counts
      JSON.parse(@client.call('queue.counts', @name))
    end

    def heartbeat
      get_config :heartbeat
    end

    def heartbeat=(value)
      set_config :heartbeat, value
    end

    def throttle
      @throttle ||= Reqless::Throttle.new("ql:q:#{name}", client)
    end

    def paused?
      counts['paused']
    end

    def pause(opts = {})
      @client.call('queue.pause', name)
      @client.call('job.timeout', jobs.running(0, -1)) unless opts[:stopjobs].nil?
    end

    def unpause
      @client.call('queue.unpause', name)
    end

    # Put the described job in this queue
    # Options include:
    # => priority (int)
    # => tags (array of strings)
    # => delay (int)
    # => throttles (array of strings)
    def put(klass, data, opts = {})
      opts = job_options(klass, data, opts)
      @client.call(
        'queue.put',
        worker_name, @name,
        (opts[:jid] || Reqless.generate_jid),
        klass.is_a?(String) ? klass : klass.name,
        *Job.build_opts_array(opts.merge(:data => data)),
      )
    end

    # Make a recurring job in this queue
    # Options include:
    # => priority (int)
    # => tags (array of strings)
    # => retries (int)
    # => offset (int)
    def recur(klass, data, interval, opts = {})
      opts = job_options(klass, data, opts)
      @client.call(
        'queue.recurAtInterval',
        @name,
        (opts[:jid] || Reqless.generate_jid),
        klass.is_a?(String) ? klass : klass.name,
        JSON.generate(data),
        interval, opts.fetch(:offset, 0),
        'priority', opts.fetch(:priority, 0),
        'tags', JSON.generate(opts.fetch(:tags, [])),
        'retries', opts.fetch(:retries, 5),
        'backlog', opts.fetch(:backlog, 0)
      )
    end

    # Pop a work item off the queue
    def pop(count = nil)
      jids = JSON.parse(@client.call('queue.pop', @name, worker_name, (count || 1)))
      jobs = jids.map { |j| Job.new(@client, j) }
      count.nil? ? jobs[0] : jobs
    end

    # Peek at a work item
    def peek(offset_or_count = nil, count = nil)
      actual_offset = offset_or_count && count ? offset_or_count : 0
      actual_count = offset_or_count && count ? count : (offset_or_count || 1)
      return_single_job = offset_or_count.nil? && count.nil?
      jids = JSON.parse(@client.call('queue.peek', @name, actual_offset, actual_count))
      jobs = jids.map { |j| Job.new(@client, j) }
      return_single_job ? jobs[0] : jobs
    end

    def stats(date = nil)
      JSON.parse(@client.call('queue.stats', @name, (date || Time.now.to_f)))
    end

    # How many items in the queue?
    def length
      (@client.redis.multi do |pipeline|
        pipeline.zcard("ql:q:#{@name}-locks")
        pipeline.zcard("ql:q:#{@name}-work")
        pipeline.zcard("ql:q:#{@name}-scheduled")
      end).inject(0, :+)
    end

    def to_s
      "#<Reqless::Queue #{@name}>"
    end
    alias_method :inspect, :to_s

    def ==(other)
      self.class == other.class &&
      client == other.client &&
      name.to_s == other.name.to_s
    end
    alias eql? ==

    def hash
      self.class.hash ^ client.hash ^ name.to_s.hash
    end

  private

    def job_options(klass, data, opts)
      return opts unless klass.respond_to?(:default_job_options)
      klass.default_job_options(data).merge(opts)
    end

    def set_config(config, value)
      @client.config["#{@name}-#{config}"] = value
    end

    def get_config(config)
      @client.config["#{@name}-#{config}"]
    end
  end
end
