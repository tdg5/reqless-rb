require 'qless'

require 'spec_helper'

module Qless
  describe ClientQueuePatterns, :integration do
    describe ClientQueuePatterns do
      describe '#get_queue_identifier_patterns' do
        it 'returns the expected default when no patterns are defined' do
          identifier_patterns = client.queue_patterns.get_queue_identifier_patterns
          expect(identifier_patterns).to eq({'default' => ['*']})
        end

        it 'returns the expected patterns when patterns are defined' do
          expected_patterns = {
            'bar' => ['foo', 'bar'],
            'baz' => ['foo', 'bar', 'baz'],
            'default' => ['baz'],
            'foo' => ['foo'],
          }
          client.queue_patterns.set_queue_identifier_patterns(expected_patterns)
          patterns = client.queue_patterns.get_queue_identifier_patterns
          expect(patterns).to eq(expected_patterns)
        end
      end

      describe '#set_queue_identifier_patterns' do
        it 'replaces any existing queue identifier patterns with the given patterns' do
          initial_patterns = {'bar' => ['bar']}
          client.queue_patterns.set_queue_identifier_patterns(initial_patterns)
          identifier_patterns = client.queue_patterns.get_queue_identifier_patterns
          expect(identifier_patterns).to eq({'bar' => ['bar'], 'default' => ['*']})

          expected_patterns = {
            'bar' => ['foo', 'bar'],
            'baz' => ['foo', 'bar', 'baz'],
            'default' => ['baz'],
            'foo' => ['foo'],
          }
          client.queue_patterns.set_queue_identifier_patterns(expected_patterns)
          patterns = client.queue_patterns.get_queue_identifier_patterns
          expect(patterns).to eq(expected_patterns)
        end

        it 'resets to default patterns if no patterns are given' do
          expected_patterns = {
            'bar' => ['foo', 'bar'],
            'baz' => ['foo', 'bar', 'baz'],
            'default' => ['baz'],
            'foo' => ['foo'],
          }
          client.queue_patterns.set_queue_identifier_patterns(expected_patterns)
          patterns = client.queue_patterns.get_queue_identifier_patterns
          expect(patterns).to eq(expected_patterns)

          client.queue_patterns.set_queue_identifier_patterns({})
          identifier_patterns = client.queue_patterns.get_queue_identifier_patterns
          expect(identifier_patterns).to eq({'default' => ['*']})
        end
      end

      describe '#get_queue_priority_patterns' do
        it 'returns the expected default when no patterns are defined' do
          priority_patterns = client.queue_patterns.get_queue_priority_patterns
          expect(priority_patterns).to eq([])
        end

        it 'returns the expected patterns when patterns are defined' do
          expected_patterns = [
            QueuePriorityPattern.new(pattern=['foo'], true),
            QueuePriorityPattern.new(pattern=['foo', 'bar'], false),
            QueuePriorityPattern.new(pattern=['foo', 'bar', 'baz'], true),
          ]
          client.queue_patterns.set_queue_priority_patterns(expected_patterns)
          priority_patterns = client.queue_patterns.get_queue_priority_patterns
          expect(priority_patterns).to eq(expected_patterns)
        end
      end

      describe '#set_queue_priority_patterns' do
        it 'replaces all existing patterns with the given patterns' do
          initial_patterns = [
            QueuePriorityPattern.new(pattern=['foo'], true),
          ]
          client.queue_patterns.set_queue_priority_patterns(initial_patterns)
          priority_patterns = client.queue_patterns.get_queue_priority_patterns
          expect(priority_patterns).to eq(initial_patterns)

          expected_patterns = [
            QueuePriorityPattern.new(pattern=['foo'], true),
            QueuePriorityPattern.new(pattern=['foo', 'bar'], false),
            QueuePriorityPattern.new(pattern=['foo', 'bar', 'baz'], true),
          ]
          client.queue_patterns.set_queue_priority_patterns(expected_patterns)
          priority_patterns = client.queue_patterns.get_queue_priority_patterns
          expect(priority_patterns).to eq(expected_patterns)
        end

        it 'clears out all existing patterns if no patterns are given' do
          initial_patterns = [QueuePriorityPattern.new(pattern=['foo'], true)]
          client.queue_patterns.set_queue_priority_patterns(initial_patterns)
          priority_patterns = client.queue_patterns.get_queue_priority_patterns
          expect(priority_patterns).to eq(initial_patterns)
          client.queue_patterns.set_queue_priority_patterns([])
          priority_patterns = client.queue_patterns.get_queue_priority_patterns
          expect(priority_patterns).to eq([])
        end
      end
    end
  end
end
