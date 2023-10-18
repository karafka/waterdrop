# frozen_string_literal: true

FactoryBot.define do
  factory :producer, class: 'WaterDrop::Producer' do
    skip_create

    deliver { true }
    logger { Logger.new('/dev/null', level: Logger::DEBUG) }
    max_wait_timeout { 30 }
    wait_on_queue_full { false }
    wait_timeout_on_queue_full { 1.0 }

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
      end

      instance.monitor.subscribe(::WaterDrop::Instrumentation::LoggerListener.new(logger))

      instance
    end
  end

  factory :transactional_producer, parent: :producer do
    transient do
      transactional_id { SecureRandom.uuid }
      transaction_timeout_ms { 30_000 }
    end

    kafka do
      {
        'bootstrap.servers': 'localhost:9092',
        'request.required.acks': 'all',
        'transactional.id': transactional_id,
        'transaction.timeout.ms': transaction_timeout_ms
      }
    end
  end

  factory :limited_producer, parent: :producer do
    wait_timeout_on_queue_full { 15 }

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
    wait_timeout_on_queue_full { 2 }

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
end
