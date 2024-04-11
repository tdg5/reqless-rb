# Encoding: utf-8

require 'spec_helper'
require 'qless/queue'
require 'qless/job_reservers/round_robin'

module Qless
  module JobReservers
    describe RoundRobin do
      let(:q1) { instance_double('Qless::Queue') }
      let(:q2) { instance_double('Qless::Queue') }
      let(:q3) { instance_double('Qless::Queue') }
      let(:reserver) { RoundRobin.new([q1, q2, q3]) }

      def stub_queue_names
        expect(q1).to receive(:name).and_return('Queue1')
        expect(q2).to receive(:name).and_return('Queue2')
        expect(q3).to receive(:name).and_return('Queue3')
      end

      describe '#reserve' do
        it 'round robins the queues' do
          expect(q1).to receive(:pop).twice { :q1_job }
          expect(q2).to receive(:pop).once  { :q2_job }
          expect(q3).to receive(:pop).once  { :q3_job }

          expect(reserver.reserve).to eq(:q1_job)
          expect(reserver.reserve).to eq(:q2_job)
          expect(reserver.reserve).to eq(:q3_job)
          expect(reserver.reserve).to eq(:q1_job)
        end

        it 'returns nil if none of the queues have jobs' do
          expect(q1).to receive(:pop).once { nil }
          expect(q2).to receive(:pop).once { nil }
          expect(q3).to receive(:pop).once { nil }
          expect(reserver.reserve).to be_nil
        end
      end

      describe '#description' do
        before { stub_queue_names }

        it 'returns a useful human readable string' do
          expect(reserver.description).to eq(
            'Queue1, Queue2, Queue3 (round robin)')
        end
      end

      describe '#reset_description!' do
        before do
          stub_queue_names
          reserver.description # to set @description
        end

        it 'sets the description to nil' do
          expect { reserver.reset_description! }.to change {
            reserver.instance_variable_get(:@description)
          }.to(nil)
        end
      end
    end
  end
end
