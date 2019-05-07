# frozen_string_literal: true

module WaterDrop
  module Schemas
    # Schema with validation rules for validating that all the message options that
    # we provide to producer ale valid and usable
    # @note Does not validate message itself as it is not our concern
    class MessageOptions < Dry::Validation::Contract
      params do
        required(:topic).filled(:str?, format?: TOPIC_REGEXP)
        optional(:key).maybe(:str?, :filled?)
        optional(:partition).filled(:int?, gteq?: 0)
        optional(:partition_key).maybe(:str?, :filled?)
        optional(:create_time).maybe(:time?)
        optional(:headers).maybe(:hash?)
      end

      class << self
        # @param options [Hash] hash with data we want to validate
        # @return [Dry::Validation::Result] dry validation execution result
        def call(options)
          new.call(options)
        end
      end
    end
  end
end
