# frozen_string_literal: true

module WaterDrop
  module Contracts
    # Contract with validation rules for WaterDrop configuration details
    class Config < Dry::Validation::Contract
      params do
        required(:logger).filled
        required(:deliver).filled(:bool?)
        required(:kafka).filled(:hash?)
        required(:max_wait_timeout).filled(:int?, gteq?: 0)
        required(:wait_timeout).filled(:float?, gt?: 0)
      end
    end
  end
end
