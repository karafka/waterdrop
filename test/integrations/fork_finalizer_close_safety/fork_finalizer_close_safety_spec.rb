# frozen_string_literal: true

# Integration test: the producer's GC finalizer must not close the parent's client when it runs in a
# forked child.
#
# On first use #client registers `ObjectSpace.define_finalizer(id, proc { close })`. That finalizer
# is inherited by a forked child. #client is fork-guarded (it raises / resets in the child), but
# #close was NOT - it had no `@pid == Process.pid` guard. So if a child inherits a used producer,
# never touches it, and exits normally, the inherited finalizer runs #close IN THE CHILD, which
# flushes and closes the parent's client. With the real rdkafka client that is rd_kafka_destroy on a
# fork-inherited handle (undefined behavior); here it is the same producer-level mistake, observed
# safely.
#
# We observe it deterministically with a tracing client wired in via the supported
# `config.client_class` setting (no WaterDrop internals are stubbed): it records every #close with
# the pid that ran it. After the child exits, the trace must NOT contain a close executed by the
# child.

require "waterdrop"
require "logger"
require "tmpdir"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")

trace_path = "#{Dir.tmpdir}/wd08-finalizer-#{Process.pid}-#{rand(1_000_000)}"
File.write(trace_path, "")
ENV["WD08_TRACE"] = trace_path

# A client that records every #close call (which pid ran it, which pid built it). Plugged in through
# the public config.client_class API - nothing internal is stubbed.
class TracingClient < WaterDrop::Clients::Dummy
  def initialize(producer)
    super
    @built_pid = Process.pid
    @trace = ENV.fetch("WD08_TRACE")
  end

  # Report the client as open so #close performs its real teardown path.
  def closed?
    false
  end

  def close
    File.open(@trace, "a") { |file| file.puts("close ran_in=#{Process.pid} built_in=#{@built_pid}") }
    self
  end
end

producer = WaterDrop::Producer.new do |config|
  config.client_class = TracingClient
  config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
  config.logger = Logger.new($stdout, level: Logger::ERROR)
end

# Use the producer in the parent: this builds the client and registers the GC finalizer.
producer.produce_sync(topic: generate_topic("wd08"), payload: "parent")

# Fork a child that inherits the already-used producer, does NOT touch it, and exits normally so the
# inherited finalizer runs at child shutdown.
child_pid = fork do
  # deliberately nothing - let the process end and run finalizers
end

Process.wait(child_pid)

# The child has fully exited (its finalizers have run). See what it did to the shared client.
lines = File.read(trace_path).each_line.map(&:strip).reject(&:empty?)
child_closes = lines.select { |line| line.include?("ran_in=#{child_pid}") }

# Now close in the parent (the legitimate owner); with the fix this is the only close that runs.
producer.close
File.delete(trace_path) if File.exist?(trace_path)

if child_closes.empty?
  puts "PASS: forked child did not close the parent's client (close lines: #{lines.inspect})"
  exit(0)
else
  warn "FAIL: child (pid #{child_pid}) closed the parent's client via the inherited finalizer: " \
       "#{child_closes.inspect}"
  exit(1)
end
