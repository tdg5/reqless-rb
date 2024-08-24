# -*- encoding: utf-8 -*-
$LOAD_PATH.push File.expand_path('../lib', __FILE__)
require 'reqless/version'

Gem::Specification.new do |s|
  s.name        = 'reqless'
  s.version     = Reqless::VERSION
  s.authors     = ['Dan Lecocq', 'Myron Marston', 'Danny Guinther']
  s.email       = ['dan@moz.com', 'myron@moz.com', 'dannyguinther@gmail.com']
  s.homepage    = 'http://github.com/tdg5/reqless-rb'
  s.summary     = %q{A Redis-Based Queueing System}
  s.description = %q{
`reqless` is meant to be a performant alternative to other queueing
systems, with statistics collection, a browser interface, and
strong guarantees about job losses.

It's written as a collection of Lua scipts that are loaded into the
Redis instance to be used, and then executed by the client library.
As such, it's intended to be extremely easy to port to other languages,
without sacrificing performance and not requiring a lot of logic
replication between clients. Keep the Lua scripts updated, and your
language-specific extension will also remain up to date.
  }

  s.files         = %w(README.md Gemfile Rakefile HISTORY.md)
  s.files        += Dir.glob('lib/**/*.rb')
  s.files        += Dir.glob('lib/reqless/lua/*.lua')
  s.files        += Dir.glob('bin/**/*')
  s.files        += Dir.glob('lib/reqless/server/**/*')
  s.bindir        = 'exe'
  s.executables   = ['reqless-web']

  s.test_files    = s.files.grep(/^(test|spec|features)\//)
  s.require_paths = ['lib']

  s.add_dependency 'redis', '~> 5.1.0'

  s.add_development_dependency 'capybara' , '~> 3.40.0'
  s.add_development_dependency 'faye-websocket', '~> 0.11.3'
  s.add_development_dependency 'launchy' , '~> 3.0.0'
  s.add_development_dependency 'metriks' , '~> 0.9'
  s.add_development_dependency 'pry'
  s.add_development_dependency 'puma' , '~> 6.4.2'
  s.add_development_dependency 'rack' , '~> 3.0.10'
  s.add_development_dependency 'rackup' , '~> 2.1.0'
  s.add_development_dependency 'rake' , '~> 13.2'
  s.add_development_dependency 'rspec' , '~> 3.13'
  s.add_development_dependency 'rubocop' , '~> 0.13.1'
  s.add_development_dependency 'rusage' , '~> 0.2.0'
  s.add_development_dependency 'selenium-webdriver' , '~> 4.23.0'
  s.add_development_dependency 'sentry-raven' , '~> 0.15'
  s.add_development_dependency 'simplecov' , '~> 0.22.0'
  s.add_development_dependency 'sinatra' , '~> 4.0.0'
  s.add_development_dependency 'timecop' , '~> 0.9.8'
end
