# frozen_string_literal: true

FactoryBot.define do
  factory :valid_message, class: 'Hash' do
    skip_create

    topic { "it-#{SecureRandom.uuid}" }
    payload { rand.to_s }
    partition_key { nil }
    label { nil }

    initialize_with do
      message = new
      message[:topic] = topic
      message[:payload] = payload
      message[:partition_key] = partition_key if partition_key
      message[:label] = label if label
      message
    end
  end

  factory :invalid_message, class: 'Hash' do
    skip_create

    initialize_with { new }
  end
end
