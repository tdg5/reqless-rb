# Encoding: utf-8

require 'logger'
require 'thread'

module Reqless
  # A class used for subscribing to messages in a thread
  class Subscriber
    def self.start(*args, &block)
      new(*args, &block).tap(&:start)
    end

    attr_reader :channel, :redis

    def initialize(client, channel, options = {}, &message_received_callback)
      @channel = channel
      @message_received_callback = message_received_callback
      @log = options.fetch(:log) { ::Logger.new($stderr) }

      # pub/sub blocks the connection so we must use a different redis
      # connection
      @client_redis   = client.redis
      @listener_redis = client.new_redis_connection

      @my_channel = Reqless.generate_jid
    end

    # Start a thread listening
    def start
      queue = ::Queue.new

      @thread = Thread.start do
        begin
          @listener_redis.subscribe(@channel, @my_channel) do |on|
            on.subscribe do |channel|
              # insert nil into the queue to indicate we've
              # successfully subscribed
              queue << nil if channel == @channel
            end

            on.message do |channel, message|
              handle_message(channel, message)
            end
          end
        # Watch for any exceptions so we don't block forever if
        # subscribing to the channel fails
        rescue Exception => e
          queue << e
        end
      end

      if (exception = queue.pop)
        raise exception
      end
    end

    def stop
      @client_redis.publish(@my_channel, 'disconnect')
      @thread.join
    end

  private

    def handle_message(channel, message)
      if channel == @my_channel
        @listener_redis.unsubscribe(@channel, @my_channel) if message == "disconnect"
      else
        @message_received_callback.call(self, JSON.parse(message))
      end
    rescue Exception => error
      @log.error("Reqless::Subscriber") { error }
    end
  end
end
