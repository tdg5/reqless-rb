require 'redis'
require 'reqless'
require 'reqless/job_reservers/ordered'
require 'reqless/job_reservers/delegating'
require 'reqless/job_reservers/strategies'
require 'spec_helper'

describe 'JobReservers::Delegating', :integration do
  before(:each) do
    client.redis.flushall
  end

  it 'should implement #queues' do
    queue_a = client.queues['a']
    queue_b = client.queues['b']

    job1 = queue_a.put(NoopJob, {})
    job2 = queue_b.put(NoopJob, {})

    expect(queue_a.length).to eq(1)
    expect(queue_b.length).to eq(1)

    reservers = []
    reservers << Reqless::JobReservers::Ordered.new([queue_a])
    reservers << Reqless::JobReservers::Ordered.new([queue_b])

    reserver = Reqless::JobReservers::Delegating.new(reservers)

    queues = reserver.queues.to_a
    expect(queues).to include(queue_a)
    expect(queues).to include(queue_b)
  end

  it 'can delegate to multiple reservers' do
    queue_a = client.queues['a']
    queue_b = client.queues['b']

    job1 = queue_a.put(NoopJob, {})
    job2 = queue_b.put(NoopJob, {})

    expect(queue_a.length).to eq(1)
    expect(queue_b.length).to eq(1)

    reserver1 = Reqless::JobReservers::Ordered.new([queue_a])
    reserver2 = Reqless::JobReservers::Ordered.new([queue_b])

    reserver = Reqless::JobReservers::Delegating.new([reserver1, reserver2])
    expect(reserver.reserve.queue.name).to eq('a')
    expect(reserver.reserve.queue.name).to eq('b')
  end

  context 'with ordering' do
    it 'should work with ordering strategy' do
      queue_a = client.queues['a']
      queue_b = client.queues['b']

      job1 = queue_a.put(NoopJob, {})
      job2 = queue_b.put(NoopJob, {})

      expect(queue_a.length).to eq(1)
      expect(queue_b.length).to eq(1)

      reservers = []
      reservers << Reqless::JobReservers::Ordered.new([queue_a])
      reservers << Reqless::JobReservers::Ordered.new([queue_b])

      reservers = Reqless::JobReservers::Strategies::Ordering.shuffled(reservers)
      reserver = Reqless::JobReservers::Delegating.new(reservers)

      expected = ['a', 'b']
      expected.delete(reserver.reserve.queue.name)
      expected.delete(reserver.reserve.queue.name)
      expect(expected).to be_empty
    end
  end
end
