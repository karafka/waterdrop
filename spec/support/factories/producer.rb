# frozen_string_literal: true

FactoryBot.define do
  factory :producer, class: 'WaterDrop::Producer' do
    skip_create

    deliver { true }
    logger { Logger.new($stdout, level: Logger::INFO) }
    kafka do
      {
        'bootstrap.servers' => 'localhost:9092',
        'request.required.acks' => 1
      }
    end


    initialize_with do
      new do |config|
        config.deliver = deliver
        config.logger = logger
        config.kafka = kafka
      end
    end
  end
end
