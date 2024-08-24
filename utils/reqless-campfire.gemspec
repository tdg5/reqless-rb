# -*- encoding: utf-8 -*-
$LOAD_PATH.push File.expand_path('../../lib', __FILE__)
require 'reqless/version'

Gem::Specification.new do |s|
  s.name        = 'reqless-campfire'
  s.version     = Reqless::VERSION
  s.authors     = ['Dan Lecocq']
  s.email       = ['dan@seomoz.org']
  s.homepage    = 'http://github.com/tdg5/reqless-rb'
  s.summary     = %q{Campfire Notifications for Reqless}
  s.description = %q{
    Get Campfire notifications for jobs you're tracking in your reqless
    queue.
  }

  s.files         = Dir.glob('../bin/reqless-campfire')
  s.executables   = ['reqless-campfire']

  s.add_dependency 'reqless'       , '~> 0.9'
  s.add_dependency 'tinder'        , '~> 1.8'
  s.add_dependency 'micro-optparse', '~> 1.1'
end
