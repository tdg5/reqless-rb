# Encoding: utf-8

# Standard stuff
require 'time'
require 'logger'
require 'thread'

require 'reqless'
require 'reqless/subscriber'

module Reqless
  module Workers
    JobLockLost = Class.new(StandardError)

    class BaseWorker
      attr_accessor :output, :reserver, :interval, :paused,
                    :options, :sighup_handler

      def initialize(reserver, options = {})
        # Our job reserver and options
        @reserver = reserver
        @options = options

        # SIGHUP handler
        @sighup_handler = options.fetch(:sighup_handler) { lambda { } }

        # Our logger
        @log = options.fetch(:logger) do
          @output = options.fetch(:output, $stdout)
          Logger.new(output).tap do |logger|
            logger.level = options.fetch(:log_level, Logger::WARN)
            logger.formatter = options.fetch(:log_formatter) do
              Proc.new { |severity, datetime, progname, msg| "#{datetime}: #{msg}\n" }
            end
          end
        end

        # The interval for checking for new jobs
        @interval = options.fetch(:interval, 5.0)
        @current_job_mutex = Mutex.new
        @current_job = nil

        # Default behavior when a lock is lost: stop after the current job.
        on_current_job_lock_lost { shutdown }
      end

      def log_level
        @log.level
      end

      def safe_trap(signal_name, &cblock)
        begin
          trap(signal_name, cblock)
        rescue ArgumentError
          warn "Signal #{signal_name} not supported."
        end
      end

      # The meaning of these signals is meant to closely mirror resque
      #
      # TERM: Shutdown immediately, stop processing jobs.
      #  INT: Shutdown immediately, stop processing jobs.
      # QUIT: Shutdown after the current job has finished processing.
      # USR1: Kill the forked children immediately, continue processing jobs.
      # USR2: Pause after this job
      # CONT: Start processing jobs again after a USR2
      #  HUP: Print current stack to log and continue
      def register_signal_handlers
        # Otherwise, we want to take the appropriate action
        trap('TERM') { exit! }
        trap('INT')  { exit! }
        safe_trap('HUP') { sighup_handler.call }
        safe_trap('QUIT') { shutdown }
        begin
          trap('CONT') { unpause }
          trap('USR2') { pause }
        rescue ArgumentError
          warn 'Signals USR2, and/or CONT not supported.'
        end
      end

      # Return an enumerator to each of the jobs provided by the reserver
      def jobs
        return Enumerator.new do |enum|
          loop do
            begin
              job = reserver.reserve
            rescue Exception => error
              # We want workers to durably stay up, so we don't want errors
              # during job reserving (e.g. network timeouts, etc) to kill the
              # worker.
              log(:error,
                "Error reserving job: #{error.class}: #{error.message}")
            end

            # If we ended up getting a job, yield it. Otherwise, we wait
            if job.nil?
              no_job_available
            else
              self.current_job = job
              enum.yield(job)
              self.current_job = nil
            end

            break if @shutdown
          end
        end
      end

      # Actually perform the job
      def perform(job)
        around_perform(job)
      rescue JobLockLost
        log(:warn, "Lost lock for job #{job.jid}")
      rescue Exception => error
        fail_job(job, error, caller)
      else
        try_complete(job)
      end

      # Allow middleware modules to be mixed in and override the
      # definition of around_perform while providing a default
      # implementation so our code can assume the method is present.
      module SupportsMiddlewareModules
        def around_perform(job)
          job.perform
        end

        def after_fork
        end
      end

      include SupportsMiddlewareModules

      # Stop processing after this job
      def shutdown
        @shutdown = true
      end
      alias stop! shutdown # so we can call `stop!` regardless of the worker type

      # Pause the worker -- take no more new jobs
      def pause
        @paused = true
        procline "Paused -- #{reserver.description}"
      end

      # Continue taking new jobs
      def unpause
        @paused = false
      end

      # Set the proceline. Not supported on all systems
      def procline(value)
        $0 = "reQless-#{Reqless::VERSION}: #{value} at #{Time.now.iso8601}"
        log(:debug, $PROGRAM_NAME)
      end

      # Complete the job unless the worker has already put it into another state
      # by completing / failing / etc. the job
      def try_complete(job)
        job.complete unless job.state_changed?
      rescue Job::CantCompleteError => e
        # There's not much we can do here. Complete fails in a few cases:
        #   - The job is already failed (i.e. by another worker)
        #   - The job is being worked on by another worker
        #   - The job has been cancelled
        #
        # We don't want to (or are able to) fail the job with this error in
        # any of these cases, so the best we can do is log the failure.
        log(:error, "Failed to complete #{job.inspect}: #{e.message}")
      end

      def fail_job(job, error, worker_backtrace)
        failure = Reqless.failure_formatter.format(job, error, worker_backtrace)
        log(:error, "Got #{failure.group} failure from #{job.inspect}\n#{failure.message}" )
        job.fail(*failure)
      rescue Job::CantFailError => e
        # There's not much we can do here. Another worker may have cancelled it,
        # or we might not own the job, etc. Logging is the best we can do.
        log(:error, "Failed to fail #{job.inspect}: #{e.message}")
      end

      def deregister
        uniq_clients.each do |client|
          client.deregister_workers(client.worker_name)
        end
      end

      def uniq_clients
        @uniq_clients ||= reserver.queues.map(&:client).uniq
      end

      def on_current_job_lock_lost(&block)
        @on_current_job_lock_lost = block
      end

      def listen_for_lost_lock(job)
        # Ensure subscribers always has a value
        subscriber = Subscriber.start(job.client, "ql:w:#{job.client.worker_name}", log: @log) do |_, message|
          if message['event'] == 'lock_lost' && message['jid'] == job.jid
            @on_current_job_lock_lost.call(job)
          end
        end

        yield
      ensure
        subscriber && subscriber.stop
      end

    private

      def log(type, msg)
        @log.public_send(type, "#{Process.pid}: #{msg}")
      end

      def no_job_available
        unless interval.zero?
          procline "Waiting for #{reserver.description}"
          log(:debug, "Sleeping for #{interval} seconds")
          sleep interval
        end
      end

      def with_current_job
        @current_job_mutex.synchronize do
          yield @current_job
        end
      end

      def current_job=(job)
        @current_job_mutex.synchronize do
          @current_job = job
        end
      end
    end
  end
end
