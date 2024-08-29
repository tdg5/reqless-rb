require 'reqless/queue_patterns_helper'

module Reqless
  module JobReservers
    module Strategies
    end
  end
end

module Reqless::JobReservers::Strategies::Filtering
  # @param [Enumerable] queues - a source of queues
  # @param [Array] regexes - a list of regexes to match against.
  # Return an enumerator of the filtered queues in
  # in prioritized order.
  def self.default(
    queues,
    regexes,
    queue_identifier_patterns,
    queue_priority_patterns
  )
    Enumerator.new do |yielder|
      # Map queues to their names
      mapped_queues = queues.reduce({}) do |hash,queue|
        hash[queue.name] = queue
        hash
      end

      # Filter the queue names against the regexes provided.
      matches = Reqless::QueuePatternsHelper.expand_queues(regexes, mapped_queues.keys, queue_identifier_patterns)

      # Prioritize the queues.
      prioritized_names = Reqless::QueuePatternsHelper.prioritize_queues(queue_priority_patterns, matches)

      prioritized_names.each do |name|
        queue = mapped_queues[name]
        if queue
          yielder << queue
        end
      end
    end
  end
end
