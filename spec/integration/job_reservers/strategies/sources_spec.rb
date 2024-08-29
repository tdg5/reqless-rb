require 'reqless'
require 'spec_helper'
require 'reqless/job_reservers/strategies/sources'


describe 'Reqless::JobReservers::Strategies::Sources', :integration do
  let(:subject) { Reqless::JobReservers::Strategies::Sources }

  before(:each) do
    client.redis.flushall
  end

  context 'direct source' do
    it 'does not return queues that have no work available' do
      no_work_queue = client.queues['no-work']
      has_work_queue = client.queues['has-work']

      no_work_queue.put(NoopJob, {})
      has_work_queue.put(NoopJob, {})

      # drain the no work queue
      no_work_queue.pop

      source = subject.direct(client)

      queues = source.collect(&:name)
      expect(queues).to include('has-work')
      expect(queues).not_to include('no-work')
    end

    it 'should not ignore queues that have work in scheduled state' do
      work_queue = client.queues['work']
      work_queue.put(NoopJob, {}, {:delay => 1600})

      %w(waiting recurring depends stalled).each do |state|
        expect(work_queue.counts[state]).to be(0)
      end
      expect(work_queue.counts['scheduled']).to be(1)

      source = subject.direct(client)

      queues = source.collect(&:name)
      expect(queues).to include('work')
    end

    it 'should not ignore queues that have work in the depends state' do
      work_queue = client.queues['work']
      jid = work_queue.put(NoopJob, {})
      work_queue.put(NoopJob, {}, {:depends => [jid]})

      work_queue.pop

      %w(waiting recurring stalled scheduled).each do |state|
        expect(work_queue.counts[state]).to be(0)
      end
      expect(work_queue.counts['depends']).to be(1)

      source = subject.direct(client)

      queues = source.collect(&:name)
      expect(queues).to include('work')
    end

    it 'should not ignore queues that have work in the recurring state' do
      work_queue = client.queues['work']
      work_queue.recur(NoopJob, {}, 1000)

      %w(waiting depends stalled scheduled).each do |state|
        expect(work_queue.counts[state]).to be(0)
      end
      expect(work_queue.counts['recurring']).to be(1)

      source = subject.direct(client)

      queues = source.collect(&:name)
      expect(queues).to include('work')
    end

    it 'should not ignore queues that have work in the waiting state' do
      work_queue = client.queues['work']
      work_queue.put(NoopJob, {})

      %w(recurring depends stalled scheduled).each do |state|
        expect(work_queue.counts[state]).to be(0)
      end
      expect(work_queue.counts['waiting']).to be(1)

      source = subject.direct(client)

      queues = source.collect(&:name)
      expect(queues).to include('work')
    end
  end

  context 'background source' do
    it 'should return the results from the delegate' do
      work_queue = client.queues['work']
      work_queue.put(NoopJob, {})
      source = subject.direct(client)
      source = subject::Background.new(source, 0.1)
      thread = source.start # Start the update
      source.stop
      thread.join

      queues = source.collect(&:name)
      expect(queues).to include('work')
    end

    context 'start' do
      it 'should update from the source' do
        work_queue = client.queues['work']
        work_queue.put(NoopJob, {})
        source = subject.direct(client)
        source = subject::Background.new(source, 0.1)
        thread = source.start # Start the update

        # Add another queue to the source
        queue = client.queues['work-queue']
        queue.put(NoopJob, {})

        # Sleep long enough for multiple updates to occur
        sleep 0.3
        source.stop
        thread.join

        queues = source.collect(&:name)
        expect(queues).to include('work')
        expect(queues).to include('work-queue')
      end
    end
  end
end
