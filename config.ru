require 'qless'
require 'qless/server'

redis_url = ENV['QLESS_WEB_REDIS_URL']
client = Qless::Client.new(:url => redis_url)
run(Qless::Server.new(client))
