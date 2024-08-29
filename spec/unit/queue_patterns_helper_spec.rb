require 'spec_helper'
require 'reqless/queue_patterns_helper'
require 'reqless/queue_priority_pattern'

describe 'QueuePatternsHelper' do
  let(:subject) { Reqless::QueuePatternsHelper }

  before(:each) do
    @real_queues = %w[high_x foo high_y superhigh_z]
  end

  context 'basic queue patterns' do
    it 'can specify simple queues' do
      expect(subject.expand_queues(%w[foo], @real_queues, {})).to(eq(%w[foo]))
      expect(subject.expand_queues(%w[foo bar], @real_queues, {})).to(eq(%w[bar foo]))
    end

    it 'can specify simple wildcard' do
      expect(subject.expand_queues(%w[*], @real_queues, {})).to(eq(%w[foo high_x high_y superhigh_z]))
    end

    it 'can include queues with pattern' do
      expect(subject.expand_queues(%w[high*], @real_queues, {})).to(eq(%w[high_x high_y]))
      expect(subject.expand_queues(%w[*high_z], @real_queues, {})).to(eq(%w[superhigh_z]))
      expect(subject.expand_queues(%w[*high*], @real_queues, {})).to(
        eq(%w[high_x high_y superhigh_z])
      )
    end

    it 'can blacklist queues' do
      expect(subject.expand_queues(%w[* !foo], @real_queues, {})).to(
        eq(%w[high_x high_y superhigh_z])
      )
    end

    it 'can blacklist queues with pattern' do
      expect(subject.expand_queues(%w[* !*high*], @real_queues, {})).to(eq(%w[foo]))
    end
  end

  context 'expanding queues' do
    it 'can dynamically lookup queues' do
      queue_identifier_patterns = {'mykey' => %w[foo bar]}
      expect(
        subject.expand_queues(%w[@mykey], @real_queues, queue_identifier_patterns)
      ).to(eq(%w[bar foo]))
    end

    it 'can blacklist dynamic queues' do
      queue_identifier_patterns = {'mykey' => %w[foo]}
      expect(
        subject.expand_queues(%w[* !@mykey], @real_queues, queue_identifier_patterns)
      ).to(eq(%w[high_x high_y superhigh_z]))
    end

    it 'can blacklist dynamic queues with negation' do
      queue_identifier_patterns = {'mykey' => %w[!foo high_x]}
      expect(
        subject.expand_queues(%w[!@mykey], @real_queues, queue_identifier_patterns)
      ).to(eq(%w[foo]))
    end

    it 'continues matching patterns following a blacklisted pattern' do
      expect(
        subject.expand_queues(%w[!f* !*high* *foo*], @real_queues, {})
      ).to(eq(%w[foo]))
    end

    it 'will not bloat the given real_queues' do
      orig = @real_queues.dup
      subject.expand_queues(%w[@mykey], @real_queues, {})
      expect(@real_queues).to(eq(orig))
    end

    it 'uses hostname as default key in dynamic queues' do
      host = Socket.gethostname
      queue_identifier_patterns = {host => %w[foo bar]}

      expect(
        subject.expand_queues(%w[@], @real_queues, queue_identifier_patterns)
      ).to(eq(%w[bar foo]))
    end

    it 'can use wildcards in dynamic queues' do
      queue_identifier_patterns = {'mykey' => %w[*high* !high_y]}
      expect(
        subject.expand_queues(%w[@mykey], @real_queues, queue_identifier_patterns)
      ).to(eq(%w[high_x superhigh_z]))
    end

    it 'falls back to default queues when missing' do
      queue_identifier_patterns = {'default' => %w[foo bar]}
      expect(
        subject.expand_queues(%w[@mykey], @real_queues, queue_identifier_patterns)
      ).to(eq(%w[bar foo]))
    end

    it 'falls back to all queues when missing and no default' do
      expect(
        subject.expand_queues(%w[@mykey], @real_queues, {})
      ).to(eq(%w[foo high_x high_y superhigh_z]))
    end

    it 'falls back to all queues when missing and no default and keep up to date' do
      expect(
        subject.expand_queues(%w[@mykey], @real_queues, {})
      ).to(eq(%w[foo high_x high_y superhigh_z]))
      @real_queues << 'bar'
      expect(
        subject.expand_queues(
          %w[@mykey], @real_queues, {})
      ).to(eq(%w[bar foo high_x high_y superhigh_z]))
    end
  end

  context 'queue priorities' do
    it 'should pick up all queues with default priority' do
      priority_buckets = [Reqless::QueuePriorityPattern.new(%w[default], false)]
      expect(
        subject.prioritize_queues(priority_buckets, @real_queues)
      ).to(eq(%w[high_x foo high_y superhigh_z]))
    end

    it 'should pick up all queues fairly' do
      # do a bunch to reduce likelyhood of random match causing test failure
      @real_queues = 50.times.collect { |i| "auto_#{i}" }
      priority_buckets = [Reqless::QueuePriorityPattern.new(%w[default], true)]
      expect(
        subject.prioritize_queues(priority_buckets, @real_queues)
      ).not_to(eq(@real_queues.sort))
      expect(
        subject.prioritize_queues(priority_buckets, @real_queues).sort
      ).to(eq(@real_queues.sort))
    end

    it 'should prioritize simple pattern' do
      priority_buckets = [
        Reqless::QueuePriorityPattern.new(%w[superhigh_z], false),
        Reqless::QueuePriorityPattern.new(%w[default], false),
      ]
      expect(
        subject.prioritize_queues(priority_buckets, @real_queues)
      ).to(eq(%w[superhigh_z high_x foo high_y]))
    end

    it 'should prioritize multiple simple patterns' do
      priority_buckets = [
        Reqless::QueuePriorityPattern.new(%w[superhigh_z], false),
        Reqless::QueuePriorityPattern.new(%w[default], false),
        Reqless::QueuePriorityPattern.new(%w[foo], false),
      ]
      expect(
        subject.prioritize_queues(priority_buckets, @real_queues)
      ).to(eq(%w[superhigh_z high_x high_y foo]))
    end

    it 'should prioritize simple wildcard pattern' do
      priority_buckets = [
        Reqless::QueuePriorityPattern.new(%w[high*], false),
        Reqless::QueuePriorityPattern.new(%w[default], false),
      ]
      expect(
        subject.prioritize_queues(priority_buckets, @real_queues)
      ).to(eq(%w[high_x high_y foo superhigh_z]))
    end

    it 'should prioritize simple wildcard pattern with correct matching' do
      priority_buckets = [
        Reqless::QueuePriorityPattern.new(%w[*high*], false),
        Reqless::QueuePriorityPattern.new(%w[default], false),
      ]
      expect(
        subject.prioritize_queues(priority_buckets, @real_queues)
      ).to(eq(%w[high_x high_y superhigh_z foo]))
    end

    it 'should prioritize negation patterns' do
      @real_queues.delete('high_x')
      @real_queues << 'high_x'
      priority_buckets = [
        Reqless::QueuePriorityPattern.new(%w[high* !high_x], false),
        Reqless::QueuePriorityPattern.new(%w[default], false),
      ]
      expect(
        subject.prioritize_queues(priority_buckets, @real_queues)
      ).to(eq(%w[high_y foo superhigh_z high_x]))
    end

    it 'should not be affected by standalone negation patterns' do
      priority_buckets = [
        Reqless::QueuePriorityPattern.new(%w[!high_x], false),
        Reqless::QueuePriorityPattern.new(%w[default], false),
      ]
      expect(
        subject.prioritize_queues(priority_buckets, @real_queues)
      ).to(eq(%w[high_x foo high_y superhigh_z]))
    end

    it 'should allow multiple inclusive patterns' do
      priority_buckets = [
        Reqless::QueuePriorityPattern.new(%w[high_x superhigh*], false),
        Reqless::QueuePriorityPattern.new(%w[default], false),
      ]
      expect(
        subject.prioritize_queues(priority_buckets, @real_queues)
      ).to(eq(%w[high_x superhigh_z foo high_y]))
    end

    it 'should prioritize fully inclusive wildcard pattern' do
      priority_buckets = [
        Reqless::QueuePriorityPattern.new(%w[*high*], false),
        Reqless::QueuePriorityPattern.new(%w[default], false),
      ]
      expect(
        subject.prioritize_queues(priority_buckets, @real_queues)
      ).to(eq(%w[high_x high_y superhigh_z foo]))
    end

    it 'should handle empty default match' do
      priority_buckets = [
        Reqless::QueuePriorityPattern.new(%w[*], false),
        Reqless::QueuePriorityPattern.new(%w[default], false),
      ]
      expect(
        subject.prioritize_queues(priority_buckets, @real_queues)
      ).to(eq(%w[high_x foo high_y superhigh_z]))
    end

    it 'should pickup wildcard queues fairly' do
      others = 5.times.collect { |i| "other#{i}" }
      @real_queues = @real_queues + others

      priority_buckets = [
        Reqless::QueuePriorityPattern.new(%w[other*], true),
        Reqless::QueuePriorityPattern.new(%w[default], false),
      ]
      queues = subject.prioritize_queues(priority_buckets, @real_queues)

      expect(queues[0..4].sort).to eq(others.sort)
      expect(queues[5..-1]).to eq(%w[high_x foo high_y superhigh_z])
      expect(queues).not_to eq(others.sort + %w[high_x foo high_y superhigh_z])
    end
  end
end
