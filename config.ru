require 'reqless'
require 'reqless/server'

redis_url = ENV['REQLESS_WEB_REDIS_URL']
client = Reqless::Client.new(:url => redis_url)
use Rack::RewindableInput::Middleware
run(Reqless::Server.new(client))
