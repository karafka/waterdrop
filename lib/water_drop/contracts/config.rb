# frozen_string_literal: true

module WaterDrop
  module Contracts
    # Contract with validation rules for WaterDrop configuration details
    class Config < Dry::Validation::Contract
      params do
        required(:id).filled(:str?)
        required(:logger).filled
        required(:deliver).filled(:bool?)
        required(:kafka).filled(:hash?)
        required(:max_wait_timeout).filled(:number?, gteq?: 0)
        required(:wait_timeout).filled(:number?, gt?: 0)
      end
    end
  end
end
