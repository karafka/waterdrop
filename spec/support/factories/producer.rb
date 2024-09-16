# frozen_string_literal: true

FactoryBot.define do
  factory :producer, class: 'WaterDrop::Producer' do
    skip_create

    deliver { true }
    logger { Logger.new('/dev/null', level: Logger::DEBUG) }
    max_wait_timeout { 30_000 }
    wait_on_queue_full { false }
    wait_timeout_on_queue_full { 1_000 }
    max_payload_size { 1_000_012 }

    kafka do
      {
        'bootstrap.servers': 'localhost:9092',
        # We emit statistics as it is a great way to check they actually always work
        'statistics.interval.ms': 100,
        'request.required.acks': 'all'
      }
    end

    initialize_with do
      instance = new do |config|
        config.deliver = deliver
        config.logger = logger
        config.kafka = kafka
        config.max_wait_timeout = max_wait_timeout
        config.wait_on_queue_full = wait_on_queue_full
        config.wait_timeout_on_queue_full = wait_timeout_on_queue_full
        config.max_payload_size = max_payload_size
      end

      instance.monitor.subscribe(::WaterDrop::Instrumentation::LoggerListener.new(logger))

      instance
    end
  end

  factory :transactional_producer, parent: :producer do
    transient do
      transactional_id { SecureRandom.uuid }
      transaction_timeout_ms { 30_000 }
      request_required_acks { 'all' }
      idempotent { true }
      queue_buffering_max_ms { 5 }
    end

    kafka do
      {
        'bootstrap.servers': 'localhost:9092',
        'request.required.acks': request_required_acks,
        'transactional.id': transactional_id,
        'transaction.timeout.ms': transaction_timeout_ms,
        'message.timeout.ms': transaction_timeout_ms,
        'enable.idempotence': idempotent,
        'queue.buffering.max.ms': queue_buffering_max_ms
      }
    end
  end

  factory :limited_producer, parent: :producer do
    wait_timeout_on_queue_full { 15_000 }

    kafka do
      {
        'bootstrap.servers': 'localhost:9092',
        'request.required.acks': 1,
        # Those will cause inline buffer overflow
        'queue.buffering.max.messages': 1,
        'queue.buffering.max.ms': 10_000
      }
    end
  end

  factory :slow_producer, parent: :producer do
    wait_timeout_on_queue_full { 2_000 }

    kafka do
      {
        'bootstrap.servers': 'localhost:9092',
        'request.required.acks': 1,
        'queue.buffering.max.messages': 1,
        'queue.buffering.max.ms': 1_000
      }
    end
  end

  factory :idempotent_producer, parent: :producer do
    kafka do
      {
        'bootstrap.servers': 'localhost:9092',
        'statistics.interval.ms': 100,
        'request.required.acks': 'all',
        'enable.idempotence': true
      }
    end
  end

  factory :unreachable_producer, parent: :producer do
    max_wait_timeout { 2_000 }

    transient do
      message_timeout_ms { 1_000 }
    end

    kafka do
      {
        'bootstrap.servers': 'localhost:9093',
        'request.required.acks': 'all',
        'transaction.timeout.ms': message_timeout_ms,
        'message.timeout.ms': message_timeout_ms
      }
    end
  end
end
