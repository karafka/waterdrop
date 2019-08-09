# frozen_string_literal: true

module WaterDrop
  module Contracts
    # Contract with validation rules for WaterDrop configuration details
    class Config < Dry::Validation::Contract
      private

      params do
        required(:logger).filled
        required(:deliver).filled(:bool?)
        required(:kafka).filled(:hash?)
        required(:wait_timeout).filled(:int?, gt?: 0)
      end
    end
  end
end
