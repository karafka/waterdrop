# frozen_string_literal: true

FactoryBot.define do
  factory :producer, class: 'WaterDrop::Producer' do
    skip_create

    initialize_with do
      new do |config|
        config.logger = Logger.new($stdout, level: Logger::DEBUG)
        config.kafka = {
          'bootstrap.servers' => 'localhost:9092',
          'request.required.acks' => 1
        }
      end
    end
  end
end
