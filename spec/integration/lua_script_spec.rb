# Encoding: utf-8

require 'reqless'

require 'spec_helper'

module Reqless
  describe LuaScript, :integration do
    let(:redis) { client.redis }
    let(:script) { LuaScript.new('reqless', redis) }

    it 'does not make any redis requests upon initialization' do
      # Should not be invoking anything on redis
      expect {
        LuaScript.new('reqless', double('Redis'))
      }.not_to raise_error
    end

    it 'can issue commands without reloading the script' do
      # Create a LuaScript object, and ensure the script is loaded
      redis.script(:load, script.send(:script_contents))
      expect(redis).to_not receive(:script)
      expect {
        script.call([], ['config.set', 12345, 'key', 3])
      }.to change { redis.keys.size }.by(1)
    end

    it 'loads the script as needed if the command fails' do
      # Ensure redis has no scripts loaded, and then invoke the command
      redis.script(:flush)
      expect(redis).to receive(:script).and_call_original
      expect {
        script.call([], ['config.set', 12345, 'key', 3])
      }.to change { redis.keys.size }.by(1)
    end

    it 're-raises non user_script errors' do
      FooError = Class.new(Redis::CommandError)
      expect(script).to receive(:_call).and_raise(FooError.new)
      expect {
        script.call([], ['foo'])
      }.to raise_error(FooError)
    end
  end

  describe LuaPlugin, :integration do
    let(:script) do
      "-- some comments\n return Reqless.config.get(ARGV[1]) * ARGV[2]"
    end
    let(:plugin) { LuaPlugin.new("my_plugin", redis, script) }

    it 'supports Reqless lua plugins' do
      client.config['heartbeat'] = 14
      expect(plugin.call('heartbeat', 3)).to eq(14 * 3)
    end

    RSpec::Matchers.define :string_excluding do |snippet|
      match do |actual|
        !actual.include?(snippet)
      end
    end

    it 'strips out comment lines before sending the script to redis' do
      expect(redis).to receive(:script)
           .with(:load, string_excluding("some comments"))
           .at_least(:once)
           .and_call_original

      client.config["heartbeat"] = 16
      expect(plugin.call "heartbeat", 3).to eq(16 * 3)
    end

    it 'does not load the script extra times' do
      expect(redis).to receive(:script)
           .with(:load, an_instance_of(String))
           .once
           .and_call_original

      3.times do
        plugin = LuaPlugin.new('my_plugin', redis, script)
        plugin.call('heartbeat', 3)
      end
    end
  end
end

