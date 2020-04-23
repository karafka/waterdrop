# frozen_string_literal: true

FactoryBot.define do
  factory :valid_message, class: 'Hash' do
    skip_create

    topic { rand.to_s }
    payload { rand.to_s }

    initialize_with do
      message = new
      message[:topic] = topic
      message[:payload] = payload
      message
    end
  end

  factory :invalid_message, class: 'Hash' do
    skip_create

    initialize_with { new }
  end
end
