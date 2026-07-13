# frozen_string_literal: true

# Helpers for reading back what we produced, so tests can assert on what a real Kafka consumer sees
# rather than only on delivery reports.
module Consumption
  # Reads all payloads currently available on a topic.
  #
  # The isolation level is the point of this helper: an aborted transaction still writes its messages
  # to the log, so `read_uncommitted` sees them while `read_committed` must not. That difference is
  # what proves a rollback actually held.
  #
  # @param topic [String] topic to read from
  # @param isolation_level [String] `read_committed` or `read_uncommitted`
  # @param timeout [Numeric] how long (in seconds) to keep polling before giving up
  # @return [Array<String>] payloads in the order they were read
  def consume_payloads(topic, isolation_level:, timeout: 10)
    consumer = Rdkafka::Config.new(
      "bootstrap.servers": BOOTSTRAP_SERVERS,
      "group.id": SecureRandom.uuid,
      "auto.offset.reset": "earliest",
      "isolation.level": isolation_level,
      "enable.partition.eof": true
    ).consumer

    consumer.subscribe(topic)

    payloads = []
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout

    while Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
      begin
        message = consumer.poll(200)
      rescue Rdkafka::RdkafkaError => e
        # We reached the end of the partition, so everything that is there is already read. For an
        # aborted transaction under `read_committed` this is how we learn there is simply nothing.
        break if e.code == :partition_eof

        raise
      end

      payloads << message.payload if message
    end

    payloads
  ensure
    consumer&.close
  end
end
