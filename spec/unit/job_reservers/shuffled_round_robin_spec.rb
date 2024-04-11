# Encoding: utf-8

require 'spec_helper'
require 'qless/queue'
require 'qless/job_reservers/shuffled_round_robin'

module Qless
  module JobReservers
    describe ShuffledRoundRobin do
      let(:q1) do
        q = instance_double('Qless::Queue')
        allow(q).to receive(:name).and_return('Queue1')
        q
      end

      let(:q2) do
        q = instance_double('Qless::Queue')
        allow(q).to receive(:name).and_return('Queue2')
        q
      end

      let(:q3) do
        q = instance_double('Qless::Queue')
        allow(q).to receive(:name).and_return('Queue3')
        q
      end

      let(:queue_list) { [q1, q2, q3] }

      def new_reserver
        ShuffledRoundRobin.new(queue_list)
      end

      let(:reserver) { new_reserver }

      describe '#reserve' do
        it 'round robins the queues' do
          expect(queue_list).to receive(:shuffle).and_return(queue_list)

          expect(q1).to receive(:pop).twice.and_return(:q1_job)
          expect(q2).to receive(:pop).and_return(:q2_job)
          expect(q3).to receive(:pop).and_return(:q3_job)

          expect(reserver.reserve).to eq(:q1_job)
          expect(reserver.reserve).to eq(:q2_job)
          expect(reserver.reserve).to eq(:q3_job)
          expect(reserver.reserve).to eq(:q1_job)
        end

        it 'returns nil if none of the queues have jobs' do
          expect(q1).to receive(:pop).once.and_return(nil)
          expect(q2).to receive(:pop).once.and_return(nil)
          expect(q3).to receive(:pop).once.and_return(nil)
          expect(reserver.reserve).to be_nil
        end
      end

      it 'shuffles the queues so that things are distributed more easily' do
        order = []

        [q1, q2, q3].each do |q|
          allow(q).to receive(:pop) { order << q }
        end

        uniq_orders = 10.times.map do
          order.clear
          reserver = new_reserver
          3.times { reserver.reserve }
          order.map(&:name).join(" ")
        end.uniq!

        expect(uniq_orders.length).to be >= 3
      end

      it 'does not change the passed queue list as a side effect' do
        orig_list = queue_list.dup

        10.times do
          new_reserver
          expect(queue_list).to eq(orig_list)
        end
      end

      describe '#prep_for_work!' do
        it 'reshuffles the queues' do
          reserver = new_reserver

          uniq_orders = 10.times.map do
            reserver.prep_for_work!
            reserver.queues.map(&:name).join(" ")
          end.uniq!

          expect(uniq_orders.length).to be >= 3
        end

        it 'resets the description to match the new queue ordering' do
          reserver = new_reserver
          initial_description = reserver.description

          reserver.prep_for_work!

          expect(reserver.description).not_to be(initial_description)
        end
      end

      describe '#description' do
        it 'returns a useful human readable string' do
          expect(queue_list).to receive(:shuffle).and_return([q2, q1, q3])

          expect(reserver.description).to eq(
            'Queue2, Queue1, Queue3 (shuffled round robin)')
        end
      end
    end
  end
end
