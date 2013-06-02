require 'resque/child_processor/basic'
require 'ffi-rzmq'

module Resque
  module ChildProcessor
    class Zmq < Basic

      def perform(job, &block)
        ctx = ZMQ::Context.create(1)
        push_sock = ctx.socket(ZMQ::PUSH)
        error_check(push_sock.setsockopt(ZMQ::LINGER, 0))
        rc = push_sock.bind('tcp://127.0.0.1:2200')
        error_check(rc)

        rc = push_sock.send_string(job.payload.to_json)
        error_check(rc)

        error_check(push_sock.close)
      end

  private

    def error_check(rc)
      if ZMQ::Util.resultcode_ok?(rc)
        false
      else
        STDERR.puts "Operation failed, errno [#{ZMQ::Util.errno}] description [#{ZMQ::Util.error_string}]"
        caller(1).each { |callstack| STDERR.puts(callstack) }
        true
      end
    end

    end
  end
end