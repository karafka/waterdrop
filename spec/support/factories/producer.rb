# frozen_string_literal: true

FactoryBot.define do
  factory :producer, class: 'WaterDrop::Producer' do
    skip_create

    deliver { true }
    logger { Logger.new($stdout, level: Logger::INFO) }
    max_wait_timeout { 30 }
    kafka do
      {
        'bootstrap.servers' => 'localhost:9092',
        # We emit statistics as it is a great way to check they actually always work
        'statistics.interval.ms' => 100,
        'request.required.acks' => 1
      }
    end

    initialize_with do
      new do |config|
        config.deliver = deliver
        config.logger = logger
        config.kafka = kafka
        config.max_wait_timeout = max_wait_timeout
      end
    end
  end
end
