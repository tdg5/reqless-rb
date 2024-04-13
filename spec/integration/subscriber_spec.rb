# Encoding: utf-8

# The things we're testing
require 'qless'
require 'qless/subscriber'

# Spec stuff
require 'spec_helper'

module Qless
  describe Subscriber, :integration, :uses_threads do
    let(:channel) { SecureRandom.uuid } # use a unique channel
    let(:logger) { StringIO.new }

    def publish(message)
      redis.publish(channel, message)
    end

    def listen
      # Start a subscriber on our test channel
      Subscriber.start(client, channel, log_to: logger) do |this, message|
        yield this, message
      end
    end

    it 'can listen for messages' do
      # Push messages onto the 'foo' key as they happen
      messages = ::Queue.new
      listen do |_, message|
        messages << message
      end

      # Wait until the message is sent
      publish('{}')
      expect(messages.pop).to eq({})
    end

    it 'does not stop listening for callback exceptions' do
      # If the callback throws an exception, it should keep listening for more
      # messages, and not fall over instead
      messages = ::Queue.new
      listen do |_, message|
        raise 'Explodify' if message['explode']
        messages << message
      end

      # Wait until the message is sent
      publish('{"explode": true}')
      publish('{}')
      expect(messages.pop).to eq({})
    end

    it 'can be stopped' do
      # We can start a listener and then stop a listener
      messages = ::Queue.new
      subscriber = listen do |_, message|
        messages << message
      end
      expect(publish('{}')).to eq(1)
      expect(messages.pop).to eq({})

      # Stop the subscriber and then ensure it's stopped listening
      subscriber.stop
      expect(publish('foo')).to eq(0)
    end
  end
end
