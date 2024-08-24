require 'reqless'
require 'reqless/test_helpers/worker_helpers'
require 'reqless/worker'
require 'reqless/job_reservers/round_robin'
require 'tempfile'

class TempfileWithString < Tempfile
  # To mirror StringIO#string
  def string
    rewind
    read.tap { close }
  end
end

shared_context "forking worker" do
  include Reqless::WorkerHelpers
  include_context "redis integration"

  let(:key) { :worker_integration_job }
  let(:queue) { client.queues['main'] }
  let(:log_io) { TempfileWithString.new('reqless.log') }
  let(:worker) do
    Reqless::Workers::ForkingWorker.new(
      Reqless::JobReservers::RoundRobin.new([queue]),
      interval: 1,
      max_startup_interval: 0,
      output: log_io,
      log_level: Logger::DEBUG)
  end

  after { log_io.unlink }
end

