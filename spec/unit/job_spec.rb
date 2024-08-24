# Encoding: utf-8

require 'spec_helper'
require 'reqless/job'

module Reqless
  describe Job do
    class JobClass
      class Nested
      end
    end

    module SomeJobMiddleware
      def around_perform(job)
        super
      end
    end

    let(:client) { double('client').as_null_object }

    describe '.build' do
      it 'creates a job instance' do
        expect(Job.build(client, JobClass)).to be_a(Job)
      end

      it 'honors attributes passed as a symbol' do
        job = Job.build(client, JobClass, data: { 'a' => 5 })
        expect(job.data).to eq('a' => 5)
      end

      it 'round-trips data through JSON to mimic real jobs' do
        time = Time.new(2012, 5, 3, 12, 30)
        job = Job.build(client, JobClass, data: { a: 5, timestamp: time })
        expect(job.data.keys).to eq(%w[ a timestamp ])
        expect(job.data['timestamp']).to be_a(String)
        expect(job.data['timestamp']).to include('2012-05-03')
      end
    end

    describe '.build_opts_array' do
      it 'should return a correctly built array' do
        # [data, delay, priority, priority_value, tags, tags_value, retries, retries_value, depends, depends_value,
        #  throttles, throttles_value]
        expected = ["{}", 0, "priority", 0, "tags", "[]", "retries", 5, "depends", "[]", "throttles", "[]"]
        job = Job.build(client, JobClass)
        expect(Job.build_opts_array(job.enqueue_opts)).to eq(expected)
      end
    end

    describe '.enqueue_opts' do
      it 'return available fields for enqueuing the job' do
        expected_fields = [:data, :priority, :tags, :retries, :depends, :throttles]
        job = Job.build(client, JobClass)
        opts = job.enqueue_opts
        expected_fields.each do |k|
          expect(opts.has_key?(k)).to(be(true))
        end

        expect(opts.keys.length).to(equal(expected_fields.length))
      end
    end

    describe '#klass' do
      it 'returns the class constant' do
        job = Job.build(client, JobClass, data: {})
        expect(job.klass).to be(JobClass)
      end

      it 'raises a useful error when the class constant is not loaded' do
        stub_const('MyJobClass', Class.new)
        job = Job.build(client, ::MyJobClass, data: {})
        hide_const('MyJobClass')
        expect { job.klass }.to raise_error(NameError, /constant MyJobClass/)
      end
    end

    describe '#perform' do
      it 'calls #perform method on the job class with job as an argument' do
        job = Job.build(client, JobClass)
        expect(JobClass).to receive(:perform).with(job).once
        job.perform
      end

      it 'properly finds nested classes' do
        job = Job.build(client, JobClass::Nested)
        expect(JobClass::Nested).to receive(:perform).with(job).once
        job.perform
      end

      context 'when the job class is a Reqless::Job::SupportsMiddleware' do
        it 'calls #around_perform on the job to run the middleware chain' do
          klass = Class.new { extend Reqless::Job::SupportsMiddleware }
          stub_const('MyJobClass', klass)

          job = Job.build(client, klass)
          expect(klass).to receive(:around_perform).with(job).once
          job.perform
        end
      end

      context('when the job mixes in middleware but forgot' +
        'Reqless::Job::SupportsMiddleware') do
        it('raises an error to alert the user to the fact they need' +
          'Reqless::Job::SupportsMiddleware') do
          klass = Class.new { extend SomeJobMiddleware }
          stub_const('MyJobClass', klass)
          job = Job.build(client, klass)

          expect do
            job.perform
          end.to raise_error(Reqless::Job::MiddlewareMisconfiguredError)
        end
      end
    end

    describe '#middlewares_on' do
      it 'returns the list of middleware mixed into the job' do
        klass = Class.new do
          extend Reqless::Job::SupportsMiddleware
          extend SomeJobMiddleware
        end

        expect(Reqless::Job.middlewares_on(klass)).to eq([
          SomeJobMiddleware, Reqless::Job::SupportsMiddleware
        ])
      end
    end

    shared_examples_for 'a method that calls lua' do |error, method, api_call, *args|
      it "raises a #{error} if a lua error is raised" do
        expect(client).to receive(:call) do |command, *a|
          expect(command).to eq(api_call)
          raise LuaScriptError.new('failed')
        end

        job = Job.build(client, Reqless::Job)

        expect do
          job.public_send(method, *args)
        end.to raise_error(error, 'failed')
      end

      it 'allows other errors to propagate' do
        expect(client).to receive(:call) do |command, *a|
          expect(command).to eq(api_call)
          raise NoMethodError
        end

        job = Job.build(client, Reqless::Job)

        expect do
          job.public_send(method, *args)
        end.to raise_error(NoMethodError)
      end
    end

    describe '#complete' do
      include_examples 'a method that calls lua',
                       Job::CantCompleteError, :complete, 'job.complete'
    end

    describe '#fail' do
      include_examples 'a method that calls lua',
                       Job::CantFailError, :fail, 'job.fail', 'group', 'message'
    end

    [
     [:fail, 'group', 'message'],
     [:complete],
     [:cancel],
     [:requeue, 'queue'],
     [:retry],
     [:retry, 55]
    ].each do |meth, *args|
      describe "##{meth}" do
        let(:job) { Job.build(client, JobClass) }

        it 'updates #state_changed? from false to true' do
          expect do
            job.send(meth, *args)
          end.to change(job, :state_changed?).from(false).to(true)
        end

        class MyCustomError < StandardError; end

        it 'does not update #state_changed? if redis connection error' do
          expect(client).to receive(:call).and_raise(MyCustomError.new('boom'))

          expect do
            job.send(meth, *args)
          end.to raise_error(MyCustomError)

          expect(job.state_changed?).to be false
        end

        it 'triggers before and after callbacks' do
          events = []

          allow(client).to receive(:call) { events << :lua_call }

          job.send(:"before_#{meth}") { |job| events << [:before_1, job] }
          job.send(:"before_#{meth}") { |job| events << [:before_2, job] }
          job.send(:"after_#{meth}")  { |job| events << [:after_1, job] }
          job.send(:"after_#{meth}")  { |job| events << [:after_2, job] }

          job.send(meth, *args)

          expect(events).to eq([
            [:before_1, job],
            [:before_2, job],
            :lua_call,
            [:after_2, job],
            [:after_1, job]
          ])
        end
      end
    end

    describe '#to_hash' do
      it 'returns out the state of the job' do
        job = Job.build(client, JobClass, 'spawned_from_jid' => 'foo')

        expect(job.to_hash).to include(
          klass_name: "Reqless::JobClass",
          state: "running",
          spawned_from_jid: "foo"
        )
      end

      it 'returns the throttles of the job' do
        job = Job.build(client, JobClass, 'throttles' => ['my-throttle'])

        expect(job.to_hash).to include(
          klass_name: "Reqless::JobClass",
          state: "running",
          throttles: ['my-throttle']
        )
      end
    end

    describe '#inspect' do
      let(:job) { Job.build(client, JobClass) }

      it 'includes the jid' do
        expect(job.inspect).to include(job.jid)
      end

      it 'includes the job class' do
        expect(job.inspect).to include(job.klass_name)
      end

      it 'includes the job queue' do
        expect(job.inspect).to include(job.queue_name)
      end
    end

    describe 'history methods' do
      let(:time_1) { Time.utc(2012, 8, 1, 12, 30) }
      let(:time_2) { Time.utc(2012, 8, 1, 12, 31) }

      let(:history_event) do
        {
          'popped' => time_2.to_i,
          'put'    => time_1.to_f,
          'q'      => 'test_error',
          'worker' => 'Myrons-Macbook-Pro.local-44396' }
      end

      let(:job) do
        Reqless::Job.build(client, JobClass, history: [history_event])
      end

      it 'returns the raw history from `raw_queue_history`' do
        expect(job.raw_queue_history).to eq([history_event])
      end

      it 'returns the raw history from `history` as well' do
        allow(job).to receive(:warn)
        expect(job.history).to eq([history_event])
      end

      it 'prints a deprecation warning from `history`' do
        expect(job).to receive(:warn).with(/deprecated/i)
        job.history
      end

      it 'converts timestamps to Time objects for `queue_history`' do
        converted = history_event.merge('popped' => time_2, 'put' => time_1)
        expect(job.queue_history).to eq([converted])
      end
    end

    describe '#initially_put_at' do
      let(:time_1) { Time.utc(2012, 8, 1, 12, 30) }
      let(:time_2) { Time.utc(2012, 8, 1, 12, 31) }

      let(:queue_1) { { 'what' => 'put', 'when' => time_1 } }
      let(:queue_2) { { 'what' => 'put', 'when' => time_2 } }

      def build_job(*events)
        Reqless::Job.build(client, JobClass, history: events)
      end

      it 'returns the earliest `put` timestamp' do
        job = build_job(queue_2, queue_1)
        expect(job.initially_put_at).to eq(time_1)
      end

      it 'tolerates queues that lack a `put` time' do
        job = build_job({}, queue_1)
        expect(job.initially_put_at).to eq(time_1)
      end
    end

    describe "equality" do
      it 'is considered equal when the reqless client and jid are equal' do
        job1 = Reqless::Job.build(client, JobClass, jid: "foo")
        job2 = Reqless::Job.build(client, JobClass, jid: "foo")

        expect(job1 == job2).to eq(true)
        expect(job2 == job1).to eq(true)
        expect(job1.eql? job2).to eq(true)
        expect(job2.eql? job1).to eq(true)

        expect(job1.hash).to eq(job2.hash)
      end

      it 'is not considered equal when the jid differs' do
        job1 = Reqless::Job.build(client, JobClass, jid: "foo")
        job2 = Reqless::Job.build(client, JobClass, jid: "food")

        expect(job1 == job2).to eq(false)
        expect(job2 == job1).to eq(false)
        expect(job1.eql? job2).to eq(false)
        expect(job2.eql? job1).to eq(false)

        expect(job1.hash).not_to eq(job2.hash)
      end

      it 'is not considered equal when the client differs' do
        job1 = Reqless::Job.build(client, JobClass, jid: "foo")
        job2 = Reqless::Job.build(double, JobClass, jid: "foo")

        expect(job1 == job2).to eq(false)
        expect(job2 == job1).to eq(false)
        expect(job1.eql? job2).to eq(false)
        expect(job2.eql? job1).to eq(false)

        expect(job1.hash).not_to eq(job2.hash)
      end

      it 'is not considered equal to other types of objects' do
        job1 = Reqless::Job.build(client, JobClass, jid: "foo")
        job2 = Class.new(Reqless::Job).build(client, JobClass, jid: "foo")

        expect(job1 == job2).to eq(false)
        expect(job1.eql? job2).to eq(false)
        expect(job1.hash).not_to eq(job2.hash)
      end
    end
  end
end
