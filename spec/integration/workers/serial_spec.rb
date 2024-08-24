# Encoding: utf-8

require 'reqless'
require 'reqless/worker/serial'
require 'reqless/job_reservers/round_robin'
require 'reqless/middleware/retry_exceptions'

require 'spec_helper'
require 'reqless/test_helpers/worker_helpers'

module Reqless
  describe Workers::SerialWorker, :integration do
    include Reqless::WorkerHelpers

    let(:key) { :worker_integration_job }
    let(:queue) { client.queues['main'] }
    let(:output) { StringIO.new }
    let(:worker) do
      Reqless::Workers::SerialWorker.new(
        Reqless::JobReservers::RoundRobin.new([queue]),
        interval: 1,
        max_startup_interval: 0,
        output: output,
        log_level: Logger::DEBUG)
    end

    it 'does not leak threads' do
      job_class = Class.new do
        def self.perform(job)
          Redis.new(url: job['redis']).rpush(job['key'], job['word'])
        end
      end
      stub_const('JobClass', job_class)

      # Put in a single job
      queue.put('JobClass', { redis: redis.id, key: key, word: 'hello' })
      expect do
        run_jobs(worker, 1) do
          expect(redis.brpop(key, timeout: 1)).to eq([key.to_s, 'hello'])
        end
      end.not_to change { Thread.list }
    end

    it 'can start a worker and then shut it down' do
      # A job that just puts a word in a redis list to show that its done
      job_class = Class.new do
        def self.perform(job)
          Redis.new(url: job['redis']).rpush(job['key'], job['word'])
        end
      end
      stub_const('JobClass', job_class)

      # Make jobs for each word
      words = %w{foo bar howdy}
      words.each do |word|
        queue.put('JobClass', { redis: redis.id, key: key, word: word })
      end

      # Wait for the job to complete, and then kill the child process
      run_jobs(worker, 3) {}

      job_results = redis.lrange(key, 0, -1)
      words.each do |word|
        expect(job_results).to include word
      end
    end

    it 'does not keep `current_job` set at the last job when it is in a sleep loop' do
      job_class = Class.new do
        def self.perform(job)
          job.client.redis.rpush(job['key'], 'OK')
        end
      end
      stub_const('JobClass', job_class)
      queue.put('JobClass', { key: key })

      run_worker_concurrently_with(worker) do
        expect(redis.brpop(key, timeout: 1)).to eq([key.to_s, "OK"])
      end

      expect { |b| worker.send(:with_current_job, &b) }.to yield_with_args(nil)
    end

    context 'when a job times out', :uses_threads do
      it 'invokes the given callback when the current job is the one that timed out' do
        callback_invoked = false
        worker.on_current_job_lock_lost do
          callback_invoked = true
          Thread.main.raise(Workers::JobLockLost)
        end

        # Job that sleeps for a while on the first pass
        class TimeOutCallbackTestJobClass
          def self.perform(job)
            redis = Redis.new(url: job['redis'])
            if redis.get(job.jid).nil?
              redis.set(job.jid, '1')
              redis.rpush(job['key'], job['word'])
              sleep 5
              job.fail('foo', 'bar')
            else
              job.complete
              redis.rpush(job['key'], job['word'])
            end
          end
        end

        # Put this job into the queue and then busy-wait for the job to be
        # running, time it out, then make sure it eventually completes
        queue.put(TimeOutCallbackTestJobClass, { redis: redis.id, key: key, word: :foo },
                  jid: 'jid')
        run_jobs(worker, 2) do
          expect(redis.brpop(key, timeout: 1)).to eq([key.to_s, 'foo'])
          client.jobs['jid'].timeout
          expect(redis.brpop(key, timeout: 1)).to eq([key.to_s, 'foo'])
          expect(client.jobs['jid'].state).to eq('complete')
        end

        expect(callback_invoked).to be true
      end

      it 'does not invoke the given callback when a different job timed out' do
        callback_invoked = false
        worker.on_current_job_lock_lost { callback_invoked = true }

        # Job that sleeps for a while on the first pass
        job_class = Class.new do
          def self.perform(job)
            job.client.redis.rpush(job['key'], 'continue')
            sleep 2
          end
        end
        stub_const('JobClass', job_class)

        queue.put('JobClass', { key: key }, jid: 'jid1', priority: 100) # so it gets popped first
        queue.put('JobClass', { key: key }, jid: 'jid2', priority: 10)

        run_jobs(worker, 1) do
          expect(redis.brpop(key, timeout: 1)).to eq([key.to_s, 'continue'])
          job2 = queue.pop
          expect(job2.jid).to eq('jid2')
          job2.timeout
          Thread.main.raise("stop working")
        end

        expect(callback_invoked).to be false
      end

      it 'does not invoke the given callback when not running a job' do
        callback_invoked = false
        worker.on_current_job_lock_lost { callback_invoked = true }

        queue.put('JobClass', {})
        queue.put('JobClass', {})

        job = queue.pop
        worker.listen_for_lost_lock(job) do
          queue.pop.timeout
        end

        expect(callback_invoked).to be false
        # Subscriber logs errors to output; ensure there was no error
        expect(output.string).to eq("")
      end

      it 'does not blow up for jobs it does not have' do
        # Disable grace period
        client.config['grace-period'] = 0

        # A class that sends a message and sleeps for a bit
        class KeyWordPublisherJobClass
          def self.perform(job)
            redis = Redis.new(url: job['redis'])
            redis.rpush(job['key'], job['word'])
            redis.brpop(job['key'])
          end
        end

        # Put this job into the queue and then have the worker lose its lock
        queue.put(KeyWordPublisherJobClass, { redis: redis.id, key: key, word: :foo },
                  priority: 10, jid: 'jid')
        queue.put(KeyWordPublisherJobClass, { redis: redis.id, key: key, word: :foo },
                  priority: 5, jid: 'other')

        run_jobs(worker, 1) do
          # Busy-wait for the job to be running, and then time out another job
          expect(redis.brpop(key, timeout: 1)).to eq([key.to_s, 'foo'])
          job = queue.pop
          expect(job.jid).to eq('other')
          job.timeout
          # And the first should still be running
          expect(client.jobs['jid'].state).to eq('running')
          redis.rpush(key, 'foo')
        end
      end
    end

    # Specs related specifically to middleware
    describe Reqless::Middleware do
      it 'will retry and eventually fail a repeatedly failing job' do
        # A job that raises an error, but automatically retries that error type
        class RetryExceptionsJobClass
          extend Reqless::Job::SupportsMiddleware
          extend Reqless::Middleware::RetryExceptions

          Kaboom = Class.new(StandardError)
          retry_on Kaboom

          def self.perform(job)
            Redis.new(url: job['redis']).rpush(job['key'], job['word'])
            raise Kaboom
          end
        end

        # Put a job and run it, making sure it gets retried
        queue.put(RetryExceptionsJobClass, { redis: redis.id, key: key, word: :foo },
                  jid: 'jid', retries: 10)
        run_jobs(worker, 1) do
          expect(redis.brpop(key, timeout: 1)).to eq([key.to_s, 'foo'])
          max_attempts = 20
          attempts = 0
          until attempts < max_attempts && client.jobs['jid'].state == 'waiting' do
            attempts += 1
            sleep 0.1
          end
        end
        expect(client.jobs['jid'].retries_left).to be < 10
      end
    end
  end
end
