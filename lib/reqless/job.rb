# Encoding: utf-8

require 'reqless'
require 'reqless/queue'
require 'reqless/lua_script'
require 'redis'
require 'json'

module Reqless
  # The base for both Job and RecurringJob
  class BaseJob
    attr_reader :client

    def initialize(client, jid)
      @client = client
      @jid    = jid
    end

    def klass
      @klass ||= @klass_name.split('::').reduce(Object) do |context, name|
        context.const_get(name, false)
      end
    end

    def queue
      @queue ||= Queue.new(@queue_name, @client)
    end

    def ==(other)
      self.class == other.class &&
      jid == other.jid &&
      client == other.client
    end
    alias eql? ==

    def hash
      self.class.hash ^ jid.hash ^ client.hash
    end
  end

  # A Reqless job
  class Job < BaseJob
    attr_reader :jid, :expires_at, :state, :queue_name, :worker_name, :failure, :spawned_from_jid
    attr_reader :klass_name, :tracked, :dependencies, :dependents
    attr_reader :original_retries, :retries_left, :raw_queue_history
    attr_reader :state_changed
    attr_accessor :data, :priority, :tags, :throttles

    alias_method(:state_changed?, :state_changed)

    MiddlewareMisconfiguredError = Class.new(StandardError)

    module SupportsMiddleware
      def around_perform(job)
        perform(job)
      end
    end

    def perform
      # If we can't find the class, we should fail the job, not try to process
      begin
        klass
      rescue NameError
        return fail("#{queue_name}-NameError", "Cannot find #{klass_name}")
      end

      # log a real process executing job -- before we start processing
      log("started by pid:#{Process.pid}")

      middlewares = Job.middlewares_on(klass)

      if middlewares.last == SupportsMiddleware
        klass.around_perform(self)
      elsif middlewares.any?
        raise MiddlewareMisconfiguredError, 'The middleware chain for ' +
              "#{klass} (#{middlewares.inspect}) is misconfigured." +
              'Reqless::Job::SupportsMiddleware must be extended onto your job' +
              'class first if you want to use any middleware.'
      elsif !klass.respond_to?(:perform)
        # If the klass doesn't have a :perform method, we should raise an error
        fail("#{queue_name}-method-missing",
             "#{klass_name} has no perform method")
      else
        klass.perform(self)
      end
    end

    def self.build(client, klass, attributes = {})
      defaults = {
        'jid'              => Reqless.generate_jid,
        'spawned_from_jid' => nil,
        'data'             => {},
        'klass'            => klass.to_s,
        'priority'         => 0,
        'tags'             => [],
        'worker'           => 'mock_worker',
        'expires'          => Time.now + (60 * 60), # an hour from now
        'state'            => 'running',
        'tracked'          => false,
        'queue'            => 'mock_queue',
        'retries'          => 5,
        'remaining'        => 5,
        'failure'          => {},
        'history'          => [],
        'dependencies'     => [],
        'dependents'       => [],
        'throttles'        => [],
      }
      attributes = defaults.merge(Reqless.stringify_hash_keys(attributes))
      attributes['data'] = JSON.dump(attributes['data'])
      new(client, attributes)
    end

    # Converts a hash of job options (as returned by job.to_hash) into the array
    # format the reqless api expects.
    def self.build_opts_array(opts)
      result = []
      result << JSON.generate(opts.fetch(:data, {}))
      result.concat([opts.fetch(:delay, 0)])
      result.concat(['priority', opts.fetch(:priority, 0)])
      result.concat(['tags', JSON.generate(opts.fetch(:tags, []))])
      result.concat(['retries', opts.fetch(:retries, 5)])
      result.concat(['depends', JSON.generate(opts.fetch(:depends, []))])
      result.concat(['throttles', JSON.generate(opts.fetch(:throttles, []))])
    end

    def self.middlewares_on(job_klass)
      singleton_klass = job_klass.singleton_class
      singleton_klass.ancestors.select do |ancestor|
        ancestor != singleton_klass && ancestor.method_defined?(:around_perform)
      end
    end

    def initialize(client, atts)
      super(client, atts.fetch('jid'))
      %w{
        data failure dependencies dependents jid priority state tags throttles
        tracked
      }.each do |att|
        instance_variable_set(:"@#{att}", atts.fetch(att))
        # Redis doesn't handle nil values so well, sometimes instead returning false,
        # so massage spawned_by_jid to consistent be nil or a jid
        @spawned_from_jid = atts.fetch('spawned_from_jid', nil) || nil
      end

      # Parse the data string
      @data = JSON.parse(@data)

      @expires_at        = atts.fetch('expires')
      @klass_name        = atts.fetch('klass')
      @queue_name        = atts.fetch('queue')
      @worker_name       = atts.fetch('worker')
      @original_retries  = atts.fetch('retries')
      @retries_left      = atts.fetch('remaining')
      @raw_queue_history = atts.fetch('history')

      # This is a silly side-effect of Lua doing JSON serialization
      @tags         = [] if @tags == {}
      @dependents   = [] if @dependents == {}
      @dependencies = [] if @dependencies == {}
      @state_changed = false
      @before_callbacks = Hash.new { |h, k| h[k] = [] }
      @after_callbacks  = Hash.new { |h, k| h[k] = [] }
    end

    def priority=(priority)
      @priority = priority if @client.call('job.setPriority', @jid, priority)
    end

    def [](key)
      @data[key]
    end

    def []=(key, val)
      @data[key] = val
    end

    def to_s
      inspect
    end

    def description
      "#{@klass_name} (#{@jid} / #{@queue_name} / #{@state})"
    end

    def inspect
      "<Reqless::Job #{description}>"
    end

    def ttl
      @expires_at - Time.now.to_f
    end

    def throttle_objects
      throttles.map { |name| Throttle.new(name, client) }
    end

    def history
      warn 'WARNING: Reqless::Job#history is deprecated; use' +
           "Reqless::Job#raw_queue_history instead; from:\n#{caller.first}"
      raw_queue_history
    end

    def queue_history
      @queue_history ||= @raw_queue_history.map do |history_event|
        history_event.each_with_object({}) do |(key, value), hash|
          # The only Numeric (Integer or Float) values we get in the history
          # are timestamps
          if value.is_a?(Numeric)
            hash[key] = Time.at(value).utc
          else
            hash[key] = value
          end
        end
      end
    end

    def initially_put_at
      @initially_put_at ||= history_timestamp('put', :min)
    end

    def spawned_from
      return nil if @spawned_from_jid.nil?
      @spawned_from ||= @client.jobs[@spawned_from_jid]
    end

    def to_hash
      {
        jid: jid,
        spawned_from_jid: spawned_from_jid,
        expires_at: expires_at,
        state: state,
        queue_name: queue_name,
        history: raw_queue_history,
        worker_name: worker_name,
        failure: failure,
        klass_name: klass_name,
        tracked: tracked,
        dependencies: dependencies,
        dependents: dependents,
        original_retries: original_retries,
        retries_left: retries_left,
        data: data,
        priority: priority,
        tags: tags,
        throttles: throttles,
      }
    end

    # Extract the enqueue options from the job
    # @return [Hash] options
    # @option options [Integer] :retries
    # @option options [Integer] :priority
    # @option options [Array<String>] :depends
    # @option options [Array<String>] :tags
    # @option options [Array<String>] throttles
    # @option options [Hash] :data
    def enqueue_opts
      {
        retries: original_retries,
        priority: priority,
        depends: dependents,
        tags: tags,
        throttles: throttles,
        data: data,
      }
    end

    # Move this from it's current queue into another
    def requeue(queue, opts = {})
      note_state_change :requeue do
        @client.call('job.requeue', @client.worker_name, queue, @jid, @klass_name,
                     *self.class.build_opts_array(self.enqueue_opts.merge!(opts))
        )
      end
    end
    alias move requeue # for backwards compatibility

    CantFailError = Class.new(Reqless::LuaScriptError)

    # Fail a job
    def fail(group, message)
      note_state_change :fail do
        @client.call(
          'job.fail',
          @jid,
          @worker_name,
          group, message,
          JSON.dump(@data)) || false
      end
    rescue Reqless::LuaScriptError => err
      raise CantFailError.new(err.message)
    end

    # Heartbeat a job
    def heartbeat
      @expires_at = @client.call(
        'job.heartbeat',
        @jid,
        @worker_name,
        JSON.dump(@data))
    end

    CantCompleteError = Class.new(Reqless::LuaScriptError)

    # Complete a job
    # Options include
    # => next (String) the next queue
    # => delay (int) how long to delay it in the next queue
    def complete(nxt = nil, options = {})
      note_state_change :complete do
        if nxt.nil?
          @client.call(
            'job.complete', @jid, @worker_name, @queue_name, JSON.dump(@data))
        else
          @client.call('job.completeAndRequeue', @jid, @worker_name, @queue_name,
                       JSON.dump(@data), 'next', nxt, 'delay',
                       options.fetch(:delay, 0), 'depends',
                       JSON.dump(options.fetch(:depends, [])))
        end
      end
    rescue Reqless::LuaScriptError => err
      raise CantCompleteError.new(err.message)
    end

    def cancel
      note_state_change :cancel do
        @client.call('job.cancel', @jid)
      end
    end

    def track
      @client.call('job.track', @jid)
    end

    def untrack
      @client.call('job.untrack', @jid)
    end

    def tag(*tags)
      JSON.parse(@client.call('job.addTag', @jid, *tags))
    end

    def untag(*tags)
      JSON.parse(@client.call('job.removeTag', @jid, *tags))
    end

    def retry(delay = 0, group = nil, message = nil)
      note_state_change :retry do
        if group.nil?
          results = @client.call(
            'job.retry', @jid, @queue_name, @worker_name, delay)
          results.nil? ? false : results
        else
          results = @client.call(
            'job.retry', @jid, @queue_name, @worker_name, delay, group, message)
          results.nil? ? false : results
        end
      end
    end

    def depend(*jids)
      !!@client.call('job.addDependency', @jid, *jids)
    end

    def undepend(*jids)
      !!@client.call('job.removeDependency', @jid,  *jids)
    end

    def timeout
      @client.call('job.timeout', @jid)
    end

    def log(message, data = nil)
      if data
        @client.call('job.log', @jid, message, JSON.dump(data))
      else
        @client.call('job.log', @jid, message)
      end
    end

    [:fail, :complete, :cancel, :requeue, :retry].each do |event|
      define_method :"before_#{event}" do |&block|
        @before_callbacks[event] << block
      end

      define_method :"after_#{event}" do |&block|
        @after_callbacks[event].unshift block
      end
    end
    alias before_move before_requeue
    alias after_move  after_requeue

    def note_state_change(event)
      @before_callbacks[event].each { |blk| blk.call(self) }
      result = yield
      @state_changed = true
      @after_callbacks[event].each { |blk| blk.call(self) }
      result
    end

  private

    def history_timestamp(name, selector)
      items = queue_history.select do |q|
        q['what'] == name
      end
      items.map do |q|
        q['when']
      end.public_send(selector)
    end
  end

  # Wraps a recurring job
  class RecurringJob < BaseJob
    attr_reader :jid, :data, :priority, :tags, :retries, :interval, :count
    attr_reader :queue_name, :klass_name, :backlog

    def initialize(client, atts)
      super(client, atts.fetch('jid'))
      %w{jid data priority tags retries interval count backlog}.each do |att|
        instance_variable_set("@#{att}".to_sym, atts.fetch(att))
      end

      # Parse the data string
      @data        = JSON.parse(@data)
      @klass_name  = atts.fetch('klass')
      @queue_name  = atts.fetch('queue')
      @tags        = [] if @tags == {}
    end

    def priority=(value)
      @client.call('recurringJob.update', @jid, 'priority', value)
      @priority = value
    end

    def retries=(value)
      @client.call('recurringJob.update', @jid, 'retries', value)
      @retries = value
    end

    def interval=(value)
      @client.call('recurringJob.update', @jid, 'interval', value)
      @interval = value
    end

    def data=(value)
      @client.call('recurringJob.update', @jid, 'data', JSON.dump(value))
      @data = value
    end

    def klass=(value)
      @client.call('recurringJob.update', @jid, 'klass', value.to_s)
      @klass_name = value.to_s
    end

    def backlog=(value)
      @client.call('recurringJob.update', @jid, 'backlog', value.to_s)
      @backlog = value
    end

    def move(queue)
      @client.call('recurringJob.update', @jid, 'queue', queue)
      @queue_name = queue
    end
    alias requeue move # for API parity with normal jobs

    def cancel
      @client.call('recurringJob.cancel', @jid)
    end

    def tag(*tags)
      @client.call('recurringJob.addTag', @jid, *tags)
    end

    def untag(*tags)
      @client.call('recurringJob.removeTag', @jid, *tags)
    end

    def last_spawned_jid
      return nil if never_spawned?
      "#{jid}-#{count}"
    end

    def last_spawned_job
      return nil if never_spawned?
      @client.jobs[last_spawned_jid]
    end

  private

    def never_spawned?
      count.zero?
    end
  end
end
