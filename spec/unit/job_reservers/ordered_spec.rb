# Encoding: utf-8

require 'spec_helper'
require 'qless/queue'
require 'qless/job_reservers/ordered'

module Qless
  module JobReservers
    describe Ordered do
      let(:q1) { instance_double('Qless::Queue') }
      let(:q2) { instance_double('Qless::Queue') }
      let(:q3) { instance_double('Qless::Queue') }
      let(:reserver) { Ordered.new([q1, q2, q3]) }

      describe '#reserve' do
        it 'always pops jobs from the first queue as long as it has jobs' do
         expect(q1).to receive(:pop).and_return(:j1, :j2, :j3)
         expect(q2).to_not receive(:pop)
         expect(q3).to_not receive(:pop)

          expect(reserver.reserve).to eq(:j1)
          expect(reserver.reserve).to eq(:j2)
          expect(reserver.reserve).to eq(:j3)
        end

        it 'falls back to other queues when earlier queues lack jobs' do
          call_count = 1
          expect(q1).to receive(:pop).exactly(4).times do
            :q1_job if [2, 4].include?(call_count)
          end
          expect(q2).to receive(:pop).exactly(2).times do
            :q2_job if call_count == 1
          end
          expect(q3).to receive(:pop).once do
            :q3_job if call_count == 3
          end

          expect(reserver.reserve).to eq(:q2_job)
          call_count = 2
          expect(reserver.reserve).to eq(:q1_job)
          call_count = 3
          expect(reserver.reserve).to eq(:q3_job)
          call_count = 4
          expect(reserver.reserve).to eq(:q1_job)
        end

        it 'returns nil if none of the queues have jobs' do
          [q1, q2, q3].each { |q| expect(q).to receive(:pop) }
          expect(reserver.reserve).to be_nil
        end
      end

      describe '#description' do
        it 'returns a useful human readable string' do
          expect(q1).to receive(:name).and_return('Queue1')
          expect(q2).to receive(:name).and_return('Queue2')
          expect(q3).to receive(:name).and_return('Queue3')

          expect(reserver.description).to eq('Queue1, Queue2, Queue3 (ordered)')
        end
      end
    end
  end
end
