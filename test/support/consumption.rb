# frozen_string_literal: true

require "securerandom"

# Helpers for reading back what we produced, so tests can assert on what a real Kafka consumer sees
# rather than only on delivery reports.
module Consumption
  # How many consecutive empty polls (200ms each) mean "there is nothing left on any partition".
  IDLE_POLLS_UNTIL_DRAINED = 5

  # Reads all payloads currently available on a topic.
  #
  # The isolation level is the point of this helper: an aborted transaction still writes its messages
  # to the log, so `read_uncommitted` sees them while `read_committed` must not. That difference is
  # what proves a rollback actually held.
  #
  # We deliberately do NOT use `enable.partition.eof` to detect the end of the data: librdkafka
  # signals EOF once PER PARTITION, so stopping at the first EOF would silently under-read any
  # multi-partition topic (an empty partition reaching the end first would cut us off while other
  # partitions still had messages). Draining on an idle streak instead is correct for any partition
  # count.
  #
  # @param topic [String] topic to read from
  # @param isolation_level [String] `read_committed` or `read_uncommitted`
  # @param timeout [Numeric] hard cap (in seconds) on how long we keep polling
  # @return [Array<String>] payloads in the order they were read
  def consume_payloads(topic, isolation_level:, timeout: 10)
    consumer = Rdkafka::Config.new(
      "bootstrap.servers": BOOTSTRAP_SERVERS,
      "group.id": SecureRandom.uuid,
      "auto.offset.reset": "earliest",
      "isolation.level": isolation_level
    ).consumer

    consumer.subscribe(topic)

    payloads = []
    idle = 0
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout

    while Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
      message = consumer.poll(200)

      if message
        payloads << message.payload
        idle = 0

        next
      end

      # An empty poll before we have an assignment only means we are still joining the group. If we
      # counted those, we would bail out early and report an empty topic that is not empty at all.
      next if consumer.assignment.empty?

      idle += 1

      break if idle >= IDLE_POLLS_UNTIL_DRAINED
    end

    payloads
  ensure
    consumer&.close
  end
end
