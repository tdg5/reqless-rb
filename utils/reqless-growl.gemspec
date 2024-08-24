# -*- encoding: utf-8 -*-
$LOAD_PATH.push File.expand_path('../../lib', __FILE__)
require 'reqless/version'

Gem::Specification.new do |s|
  s.name        = 'reqless-growl'
  s.version     = Reqless::VERSION
  s.authors     = ['Dan Lecocq']
  s.email       = ['dan@seomoz.org']
  s.homepage    = 'http://github.com/tdg5/reqless-rb'
  s.summary     = %q{Growl Notifications for Reqless}
  s.description = %q{
    Get Growl notifications for jobs you're tracking in your reqless
    queue.
  }

  s.files         = Dir.glob('../bin/reqless-growl')
  s.executables   = ['reqless-growl']

  s.add_dependency 'reqless'       , '~> 0.9'
  s.add_dependency 'ruby-growl'    , '~> 4.0'
  s.add_dependency 'micro-optparse', '~> 1.1'
end
