# frozen_string_literal: true

module Factories
  # Producer variants factories used for specs
  module Producer
    # @param overrides [Hash] attributes we want to override
    # @return [WaterDrop::Producer] producer
    def producer_factory(overrides = {})
      defaults = {
        deliver: true,
        logger: Logger.new('/dev/null', level: Logger::DEBUG),
        max_wait_timeout: 30_000,
        wait_on_queue_full: false,
        wait_timeout_on_queue_full: 1_000,
        max_payload_size: 1_000_012,
        idle_disconnect_timeout: 0,
        kafka: {
          'bootstrap.servers': 'localhost:9092',
          'statistics.interval.ms': 100,
          'request.required.acks': 'all'
        }
      }

      attributes = defaults.merge(overrides)

      instance = WaterDrop::Producer.new do |config|
        config.deliver = attributes[:deliver]
        config.logger = attributes[:logger]
        config.kafka = attributes[:kafka]
        config.max_wait_timeout = attributes[:max_wait_timeout]
        config.wait_on_queue_full = attributes[:wait_on_queue_full]
        config.wait_timeout_on_queue_full = attributes[:wait_timeout_on_queue_full]
        config.max_payload_size = attributes[:max_payload_size]
        config.idle_disconnect_timeout = attributes[:idle_disconnect_timeout]
      end

      instance.monitor.subscribe(
        ::WaterDrop::Instrumentation::LoggerListener.new(attributes[:logger])
      )

      instance
    end

    # @param overrides [Hash] attributes we want to override
    # @return [WaterDrop::Producer] producer
    def transactional_producer_factory(overrides = {})
      transient = {
        transactional_id: SecureRandom.uuid,
        transaction_timeout_ms: 30_000,
        request_required_acks: 'all',
        idempotent: true,
        queue_buffering_max_ms: 5
      }

      transient_attrs = transient.merge(overrides.select { |k, _| transient.key?(k) })
      producer_attrs = overrides.reject { |k, _| transient.key?(k) }

      kafka_config = {
        'bootstrap.servers': 'localhost:9092',
        'request.required.acks': transient_attrs[:request_required_acks],
        'transactional.id': transient_attrs[:transactional_id],
        'transaction.timeout.ms': transient_attrs[:transaction_timeout_ms],
        'message.timeout.ms': transient_attrs[:transaction_timeout_ms],
        'enable.idempotence': transient_attrs[:idempotent],
        'queue.buffering.max.ms': transient_attrs[:queue_buffering_max_ms]
      }

      producer_factory(
        { kafka: kafka_config }.merge(producer_attrs)
      )
    end

    # @param overrides [Hash] attributes we want to override
    # @return [WaterDrop::Producer] producer
    def limited_producer_factory(overrides = {})
      kafka_config = {
        'bootstrap.servers': 'localhost:9092',
        'request.required.acks': 1,
        'queue.buffering.max.messages': 1,
        'queue.buffering.max.ms': 10_000
      }

      producer_factory(
        {
          wait_timeout_on_queue_full: 15_000,
          kafka: kafka_config
        }.merge(overrides)
      )
    end

    # @param overrides [Hash] attributes we want to override
    # @return [WaterDrop::Producer] producer
    def slow_producer_factory(overrides = {})
      kafka_config = {
        'bootstrap.servers': 'localhost:9092',
        'request.required.acks': 1,
        'queue.buffering.max.messages': 1,
        'queue.buffering.max.ms': 1_000
      }

      producer_factory(
        {
          wait_timeout_on_queue_full: 2_000,
          kafka: kafka_config
        }.merge(overrides)
      )
    end

    # @param overrides [Hash] attributes we want to override
    # @return [WaterDrop::Producer] producer
    def idempotent_producer_factory(overrides = {})
      kafka_config = {
        'bootstrap.servers': 'localhost:9092',
        'statistics.interval.ms': 100,
        'request.required.acks': 'all',
        'enable.idempotence': true
      }

      producer_factory(
        {
          kafka: kafka_config
        }.merge(overrides)
      )
    end

    # @param overrides [Hash] attributes we want to override
    # @return [WaterDrop::Producer] producer
    def unreachable_producer_factory(overrides = {})
      transient = {
        message_timeout_ms: 1_000
      }

      transient_attrs = transient.merge(overrides.select { |k, _| transient.key?(k) })
      producer_attrs = overrides.reject { |k, _| transient.key?(k) }

      kafka_config = {
        'bootstrap.servers': 'localhost:9093',
        'request.required.acks': 'all',
        'transaction.timeout.ms': transient_attrs[:message_timeout_ms],
        'message.timeout.ms': transient_attrs[:message_timeout_ms]
      }

      producer_factory(
        {
          max_wait_timeout: 5_000,
          kafka: kafka_config
        }.merge(producer_attrs)
      )
    end
  end
end
