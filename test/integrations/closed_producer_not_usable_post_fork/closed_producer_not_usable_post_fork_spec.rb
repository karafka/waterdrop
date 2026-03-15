# frozen_string_literal: true

# A producer that was closed before forking should remain closed in the child process.
# Attempting to use it should raise WaterDrop::Errors::ProducerClosedError.
# This behavior is by design - closed producers do not "resurrect" after fork.
# Users who need a working producer in a forked child must reinitialize it explicitly.

require "waterdrop"
require "logger"
require "securerandom"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

topic_name = "it-closed-fork-#{SecureRandom.hex(6)}"

producer = WaterDrop::Producer.new do |config|
  config.kafka = {
    "bootstrap.servers": BOOTSTRAP_SERVERS
  }
  config.logger = Logger.new($stdout, level: Logger::INFO)
end

# Ensure the producer has been used at least once, then close it before forking
producer.produce_sync(topic: topic_name, payload: "pre-fork")
producer.close

reader, writer = IO.pipe

pid = fork do
  reader.close

  error = begin
    producer.produce_sync(topic: topic_name, payload: "post-fork")
    nil
  rescue WaterDrop::Errors::ProducerClosedError => e
    e
  end

  if error
    writer.puts("ProducerClosedError")
  else
    writer.puts("no_error")
  end

  writer.close
  exit!(0)
end

writer.close
result = reader.gets&.strip
reader.close

Process.wait(pid)

success = result == "ProducerClosedError"

exit(success ? 0 : 1)
