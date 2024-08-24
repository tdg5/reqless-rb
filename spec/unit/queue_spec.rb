# Encoding: utf-8

require 'spec_helper'
require 'yaml'
require 'reqless/queue'

# A job class where to_s has been changed
class SomeJobClassWithDifferentToS
  def self.to_s
    'this is a different to_s'
  end
end

module Reqless
  class DefaultOptionsJobClass
    def self.default_job_options(data)
      { jid: "jid-#{data[:arg]}", priority: 100 }
    end
  end

  class OtherClass
  end

  describe Queue, :integration do
    ['name', :name].each do |name|
      it "can query length when initialized with nem = #{name.inspect}" do
        q = Queue.new(name, client)
        expect(q.length).to eq(0)
      end
    end

    [:to_s, :inspect].each do |meth|
      it "returns a human-readable string from ##{meth}" do
        q = Queue.new('queue-name', client)
        string = q.send(meth)
        expect(string.chars.length).to be <= 100
        expect(string).to include('queue-name')
      end
    end

    it 'can specify a jid in put and recur' do
      expect(client.queues['foo'].put(
        Reqless::Job, { 'foo' => 'bar' },    jid: 'howdy')).to eq('howdy')
      expect(client.queues['foo'].recur(
        Reqless::Job, { 'foo' => 'bar' }, 5, jid: 'hello')).to eq('hello')
      expect(client.jobs['howdy']).to be
      expect(client.jobs['hello']).to be
    end

    shared_examples_for 'job options' do
      let(:q) { Queue.new('q', client) }

      it "uses options provided by the class's .defualt_job_options method" do
        jid = enqueue(q, DefaultOptionsJobClass, { arg: 'foo' })
        job = client.jobs[jid]
        expect(job.jid).to eq('jid-foo')
        expect(job.priority).to eq(100)
      end

      it 'overrides the default options with the passed options' do
        jid = enqueue(q, DefaultOptionsJobClass, { arg: 'foo' }, priority: 15)
        job = client.jobs[jid]
        expect(job.priority).to eq(15)
      end

      it 'works fine when the class does not define .default_job_options' do
        jid = enqueue(q, OtherClass, { arg: 'foo' }, priority: 15)
        job = client.jobs[jid]
        expect(job.priority).to eq(15)
      end
    end

    describe '#put' do
      def enqueue(q, klass, data, opts = {})
        q.put(klass, data, opts)
      end

      include_examples 'job options'

      it "uses the class's name properly (not #to_s)" do
        q = Queue.new('q', client)
        jid = enqueue(q, SomeJobClassWithDifferentToS, {})
        job = client.jobs[jid]
        expect(job.klass_name).to eq('SomeJobClassWithDifferentToS')
      end
    end

    describe '#recur' do
      def enqueue(q, klass, data, opts = {})
        q.recur(klass, data, 10, opts)
      end

      include_examples 'job options'
    end

    describe "#throttle" do
      let(:q) { Queue.new('a_queue', client) }

      it "returns a Reqless::Throttle" do
        expect(q.throttle).to be_a(Reqless::Throttle)
      end

      it "mirrors updates correctly" do
        expect(q.throttle.maximum).to eq(0)
        t = Throttle.new('ql:q:a_queue', client)
        expect(t.maximum).to eq(0)

        t.maximum = 3
        expect(q.throttle.maximum).to eq(3)

        q.throttle.maximum = 5
        expect(t.maximum).to eq(5)
      end
    end

    describe "equality" do
      it 'is considered equal when the reqless client and name are equal' do
        q1 = Reqless::Queue.new('foo', client)
        q2 = Reqless::Queue.new('foo', client)

        expect(q1 == q2).to eq(true)
        expect(q2 == q1).to eq(true)
        expect(q1.eql? q2).to eq(true)
        expect(q2.eql? q1).to eq(true)

        expect(q1.hash).to eq(q2.hash)
      end

      it 'is considered equal when the reqless client is the same and the names only differ in symbol vs string' do
        q1 = Reqless::Queue.new('foo', client)
        q2 = Reqless::Queue.new(:foo, client)

        expect(q1 == q2).to eq(true)
        expect(q2 == q1).to eq(true)
        expect(q1.eql? q2).to eq(true)
        expect(q2.eql? q1).to eq(true)

        expect(q1.hash).to eq(q2.hash)
      end

      it 'is not considered equal when the name differs' do
        q1 = Reqless::Queue.new('foo', client)
        q2 = Reqless::Queue.new('food', client)

        expect(q1 == q2).to eq(false)
        expect(q2 == q1).to eq(false)
        expect(q1.eql? q2).to eq(false)
        expect(q2.eql? q1).to eq(false)

        expect(q1.hash).not_to eq(q2.hash)
      end

      it 'is not considered equal when the client differs' do
        q1 = Reqless::Queue.new('foo', client)
        q2 = Reqless::Queue.new('foo', double)

        expect(q1 == q2).to eq(false)
        expect(q2 == q1).to eq(false)
        expect(q1.eql? q2).to eq(false)
        expect(q2.eql? q1).to eq(false)

        expect(q1.hash).not_to eq(q2.hash)
      end

      it 'is not considered equal to other types of objects' do
        q1 = Reqless::Queue.new('foo', client)
        q2 = Class.new(Reqless::Queue).new('foo', client)

        expect(q1 == q2).to eq(false)
        expect(q1.eql? q2).to eq(false)
        expect(q1.hash).not_to eq(q2.hash)
      end
    end
  end
end
