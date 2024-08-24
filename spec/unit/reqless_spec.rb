# Encoding: utf-8

require 'spec_helper'
require 'reqless'

describe Reqless do
  describe '.generate_jid' do
    it 'generates a UUID suitable for use as a jid' do
      expect(Reqless.generate_jid).to match(/\A[a-f0-9]{32}\z/)
    end
  end

  def redis_double(overrides = {})
    attributes = { id: 'redis://foo:1/1', info: { 'redis_version' => '2.6.0' } }
    instance_double('Redis', attributes.merge(overrides))
  end

  let(:redis) { redis_double }
  let(:redis_class) do
    class_double('Redis').as_stubbed_const(transfer_nested_constants: true)
  end

  before do
    allow(redis_class).to receive(:new) { |*a| redis }
    allow(redis).to receive(:script) # so no scripts get loaded
  end

  describe '#worker_name' do
    it 'includes the hostname in the worker name' do
      expect(Reqless::Client.new.worker_name).to include(Socket.gethostname)
    end

    it 'includes the pid in the worker name' do
      expect(Reqless::Client.new.worker_name).to include(Process.pid.to_s)
    end
  end

  context 'when instantiated' do
    it 'does not check redis version if check is disabled' do
      expect_any_instance_of(Reqless::Client).to_not receive(:assert_minimum_redis_version)
      Reqless::Client.new({redis: redis, ensure_minimum_version: false})
    end

    it 'raises an error if the redis version is too low' do
      expect(redis).to receive(:info).and_return({ 'redis_version' => '2.5.3' })
      expect { Reqless::Client.new }.to raise_error(
        Reqless::UnsupportedRedisVersionError)
    end

    it 'does not raise an error if the redis version is sufficient' do
      expect(redis).to receive(:info).and_return({ 'redis_version' => '2.6.0' })
      Reqless::Client.new # should not raise an error
    end

    it 'does not raise an error if the redis version is a prerelease' do
      expect(redis).to receive(:info).and_return({ 'redis_version' => '2.6.8-pre2' })
      Reqless::Client.new # should not raise an error
    end

    it 'considers 2.10 sufficient 2.6' do
      expect(redis).to receive(:info).and_return({ 'redis_version' => '2.10.0' })
      Reqless::Client.new # should not raise an error
    end

    it 'allows the redis connection to be passed directly in' do
      expect(redis_class).to_not receive(:new)

      client = Reqless::Client.new(redis: redis)
      expect(client.redis).to be(redis)
    end

    it 'creates a new redis connection based on initial redis connection options' do
      options = { host: 'localhost', port: '6379', password: 'awes0me!' }
      # Create the initial client which also instantiates an initial redis connection
      client = Reqless::Client.new(options)
      # Prepare stub to ensure second connection is instantiated for original redis
      expect(redis).to receive(:dup)
      client.new_redis_connection
    end
  end

  describe "equality semantics" do
    it 'is considered equal to another instance connected to the same redis DB' do
      client1 = Reqless::Client.new(redis: redis_double(id: "redis://foo.com:1/1"))
      client2 = Reqless::Client.new(redis: redis_double(id: "redis://foo.com:1/1"))

      expect(client1 == client2).to eq(true)
      expect(client2 == client1).to eq(true)
      expect(client1.eql? client2).to eq(true)
      expect(client2.eql? client1).to eq(true)

      expect(client1.hash).to eq(client2.hash)
    end

    it 'is not considered equal to another instance connected to a different redis DB' do
      client1 = Reqless::Client.new(redis: redis_double(id: "redis://foo.com:1/1"))
      client2 = Reqless::Client.new(redis: redis_double(id: "redis://foo.com:1/2"))

      expect(client1 == client2).to eq(false)
      expect(client2 == client1).to eq(false)
      expect(client1.eql? client2).to eq(false)
      expect(client2.eql? client1).to eq(false)

      expect(client1.hash).not_to eq(client2.hash)
    end

    it 'is not considered equal to other types of objects' do
      client1 = Reqless::Client.new(redis: redis_double(id: "redis://foo.com:1/1"))
      client2 = Class.new(Reqless::Client).new(redis: redis_double(id: "redis://foo.com:1/1"))

      expect(client1 == client2).to eq(false)
      expect(client1.eql? client2).to eq(false)
      expect(client1.hash).not_to eq(client2.hash)
    end
  end
end
