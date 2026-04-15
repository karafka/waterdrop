# frozen_string_literal: true

# Repeatedly creates a WaterDrop producer, produces messages (both directly and via a variant),
# closes it, and checks that pipe file descriptors don't accumulate. This catches leaks in
# QueuePipe, Poller registration/unregistration, and variant fiber-local cleanup.
#
# On non-Linux platforms the pipe FD assertion is skipped (no /proc/self/fd), but the test
# still validates that the create/use/close cycle doesn't crash or hang.

require "waterdrop"
require "logger"
require "securerandom"

BOOTSTRAP_SERVERS = ENV.fetch("BOOTSTRAP_SERVERS", "127.0.0.1:9092")
LINUX = RUBY_PLATFORM.include?("linux")
CYCLES = 20

topic_name = generate_topic("pipe-cycle")

def count_pipe_fds
  return 0 unless LINUX

  Dir.glob("/proc/self/fd/*").count do |fd_path|
    File.readlink(fd_path).start_with?("pipe:")
  rescue Errno::ENOENT
    false
  end
end

pipe_counts = []
pipe_counts << count_pipe_fds

CYCLES.times do |i|
  producer = WaterDrop::Producer.new do |config|
    config.kafka = {
      "bootstrap.servers": BOOTSTRAP_SERVERS
    }
    config.logger = Logger.new($stdout, level: Logger::INFO)
  end

  # Produce directly
  producer.produce_sync(topic: topic_name, payload: "cycle-#{i}-direct")

  # Produce via a variant with altered acks
  variant = producer.with(topic_config: { acks: 1 })
  variant.produce_sync(topic: topic_name, payload: "cycle-#{i}-variant")

  producer.close

  # Let the poller thread fully exit before sampling
  sleep(0.1)

  pipe_counts << count_pipe_fds
end

if LINUX
  warn "Pipe FD counts per cycle:"
  pipe_counts.each_with_index { |c, i| warn "  #{i}: #{c}" }

  growth = pipe_counts.last - pipe_counts.first

  if growth > 4
    warn "FAIL: Pipe FDs grew by #{growth} over #{CYCLES} cycles (#{pipe_counts.first} -> #{pipe_counts.last})"
    exit(1)
  end

  warn "PASS: Pipe FD growth = #{growth} (within tolerance)"
else
  warn "PASS: #{CYCLES} create/use/close cycles completed without error (pipe FD check skipped on non-Linux)"
end

exit(0)
