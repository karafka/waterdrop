# frozen_string_literal: true

FactoryBot.define do
  factory :producer, class: 'WaterDrop::Producer' do
    skip_create

    deliver { true }
    logger { Logger.new('/dev/null', level: Logger::DEBUG) }
    max_wait_timeout { 30 }
    kafka do
      {
        'bootstrap.servers': 'localhost:9092',
        # We emit statistics as it is a great way to check they actually always work
        'statistics.interval.ms': 100,
        'request.required.acks': 1
      }
    end

    initialize_with do
      instance = new do |config|
        config.deliver = deliver
        config.logger = logger
        config.kafka = kafka
        config.max_wait_timeout = max_wait_timeout
      end

      instance.monitor.subscribe(::WaterDrop::Instrumentation::LoggerListener.new(logger))

      instance
    end
  end

  factory :limited_producer, parent: :producer do
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
end
