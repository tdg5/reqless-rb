# Encoding: utf-8

require 'spec_helper'
require 'qless/middleware/retry_exceptions'
require 'support/forking_worker_context'

module Qless
  describe Workers::ForkingWorker do
    include_context "forking worker"

    it 'can start a worker and then shut it down' do
      # A job that just puts a word in a redis list to show that its done
      class RedisMakingKeyWordPusherJobClass
        def self.perform(job)
          Redis.new(url: job['redis']).rpush(job['key'], job['word'])
        end
      end

      # Make jobs for each word
      words = %w{foo bar howdy}
      words.each do |word|
        queue.put(RedisMakingKeyWordPusherJobClass, { redis: redis.id, key: key, word: word })
      end

      # Wait for the job to complete, and then kill the child process
      run_worker_concurrently_with(worker) do
        words.each do |word|
          expect(client.redis.brpop(key, timeout: 5)).to eq([key.to_s, word])
        end
      end
    end

    it 'can drain its queues and exit' do
      class SimpleKeyWordPusherJobClass
        def self.perform(job)
          job.client.redis.rpush(job['key'], job['word'])
        end
      end

      # Make jobs for each word
      words = %w{foo bar howdy}
      words.each do |word|
        queue.put(SimpleKeyWordPusherJobClass, { key: key, word: word })
      end

      drain_worker_queues(worker)
      expect(client.redis.lrange(key, 0, -1)).to eq(words)
    end

    it 'does not blow up when the child process exits unexpectedly' do
      # A job that falls on its sword until its last retry
      class ExplodingChildrenJobClass
        def self.perform(job)
          if job.retries_left > 1
            job.retry
            Process.kill(9, Process.pid)
          else
            Redis.new(url: job['redis']).rpush(job['key'], job['word'])
          end
        end
      end

      # Put a job and run it, making sure it finally succeeds
      queue.put(ExplodingChildrenJobClass, { redis: redis.id, key: key, word: :foo },
                retries: 5)
      run_worker_concurrently_with(worker) do
        expect(client.redis.brpop(key, timeout: 5)).to eq([key.to_s, 'foo'])
      end
    end

    it 'passes along middleware to child processes' do
      # Our mixin module sends a message to a channel
      mixin = Module.new do
        define_method :around_perform do |job|
          Redis.new(url: job['redis']).rpush(job['key'], job['word'])
          super(job)
        end
      end
      worker.extend(mixin)

      # Our job class does nothing
      class NoOpJobClass
        def self.perform(job); end
      end

      # Put a job in and run it
      queue.put(NoOpJobClass, { redis: redis.id, key: key, word: :foo })
      run_worker_concurrently_with(worker) do
        expect(client.redis.brpop(key, timeout: 5)).to eq([key.to_s, 'foo'])
      end
    end

    it 'has a usable after_fork hook for use with middleware' do
      class RedisMakingKeyJobPusherJobClass
        def self.perform(job)
          Redis.new(url: job['redis']).rpush(job['key'], 'job')
        end
      end

      # Make jobs for each word
      3.times do
        queue.put(RedisMakingKeyJobPusherJobClass, { redis: redis.id, key: key })
      end

      # mixin module sends a message to a channel
      redis_url = self.redis_url
      key = self.key
      mixin = Module.new do
        define_method :after_fork do
          Redis.new(url: redis_url).rpush(key, 'after_fork')
          super()
        end
      end
      worker.extend(mixin)

      # Wait for the job to complete, and then kill the child process
      drain_worker_queues(worker)
      words = redis.lrange(key, 0, -1)
      expect(words).to eq %w[ after_fork job job job ]
    end
  end
end
