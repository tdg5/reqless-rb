module Qless
  class QueuePriorityPattern
    attr_reader :pattern, :should_distribute_fairly

    def initialize(pattern, should_distribute_fairly = false)
      @pattern = pattern
      @should_distribute_fairly = should_distribute_fairly
    end

    def ==(other)
      return self.class == other.class &&
        self.pattern.join == other.pattern.join &&
        self.should_distribute_fairly == other.should_distribute_fairly
    end
  end
end
