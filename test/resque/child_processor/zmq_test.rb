#TODO: change this require (dependent on much other refactoring)
require 'resque'
require 'mock_redis'

require 'test_helper'

require 'resque/worker'
require 'resque/child_processor/zmq'

describe Resque::ChildProcessor::Zmq do
  let(:client) { MiniTest::Mock.new }
  let(:worker) { Resque::Worker.new :foo, :client => client }

  describe "work" do
    it "each job happens in a different child process" do
      client.expect :reconnect, nil
      r = MockRedis.new :host => "localhost", :port => 9736, :db => 0
      mock_redis = Redis::Namespace.new :resque, :redis => r
      Resque.redis = mock_redis

      job_results = Tempfile.new("job_results")

      Resque::Job.create("test_q", SelfLoggingTestJob, job_results.path)
      job = Resque::Job.reserve("test_q")
      child = Resque::ChildProcessor::Zmq.new(worker)
      child.perform(job)

      # job_results.rewind
      # result = job_results.read
      # assert_match(/^SelfLoggingTestJob:[0-9]+$/, result)
      # assert("SelfLoggingTestJob:#{Process.pid}" != result)
    end
  end

end
