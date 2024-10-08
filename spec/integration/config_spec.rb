# Encoding: utf-8

require 'reqless'

require 'spec_helper'

module Reqless
  describe Config, :integration do
    it 'can set, get and erase configuration' do
      client.config['testing'] = 'foo'
      expect(client.config['testing']).to eq('foo')
      expect(client.config.all['testing']).to eq('foo')
      client.config.clear('testing')
      expect(client.config['testing']).to eq(nil)
    end

    it 'can get all configurations' do
      expect(client.config.all).to eq({
        'application'        => 'reqless',
        'grace-period'       => '10',
        'heartbeat'          => '60',
        'jobs-history'       => '604800',
        'jobs-history-count' => '50000',
        'max-job-history'    => '100',
        'max-pop-retry'      => '1',
        'max-worker-age'     => '86400',
      })
    end
  end
end
