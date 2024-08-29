require 'reqless'
require 'spec_helper'
require 'reqless/job_reservers/strategies/filtering'


describe 'Reqless::JobReservers::Strategies::Filtering', :integration do
  let(:subject) { Reqless::JobReservers::Strategies::Filtering }

  context 'default reqless filtering behavior' do
    it 'can filter multiple queues' do
      high_queue = client.queues['high']
      critical_queue = client.queues['critical']

      high_queue.put(NoopJob, {})
      critical_queue.put(NoopJob, {})

      queues = [high_queue, critical_queue]
      filter = subject.default(queues, %w[*], {}, [])

      queues = filter.collect(&:name)
      expect(queues).to include('critical')
      expect(queues).to include('high')
    end

    it 'should only return matching queues' do
      high_queue = client.queues['high']
      critical_queue = client.queues['critical']

      high_queue.put(NoopJob, {})
      critical_queue.put(NoopJob, {})

      queues = [high_queue, critical_queue]
      filter = subject.default(queues, ['critical'], {}, [])

      queues = filter.collect(&:name)
      expect(queues).to include('critical')
      expect(queues).to_not include('high')
    end

    it 'handles priorities' do
      queue_priority_patterns = [
        Reqless::QueuePriorityPattern.new(%w[foo*], false),
        Reqless::QueuePriorityPattern.new(%w[default], false),
        Reqless::QueuePriorityPattern.new(%w[bar], true),
      ]

      queues = ['other', 'blah', 'foobie', 'bar', 'foo'].reduce([]) do |queues, q|
        queue = client.queues[q]
        queue.put(NoopJob, {})
        expect(queue.length).to be(1)
        queues << queue
      end

      filter = subject.default(queues, ['*', '!blah'], {}, queue_priority_patterns)

      expect(filter.next.name).to eq('foo')
      expect(filter.next.name).to eq('foobie')
      expect(filter.next.name).to eq('other')
      expect(filter.next.name).to eq('bar')
      expect { filter.next }.to raise_error(StopIteration)
    end

    it 'should return an empty array if nothing matches the filter' do
      queue = client.queues['filtered']
      queue.put(NoopJob, {})

      filter = subject.default([queue], ['queue'], {}, [])
      expect(filter.to_a).to(eq([]))
    end
  end
end
