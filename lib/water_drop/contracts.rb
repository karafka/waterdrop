# frozen_string_literal: true

module WaterDrop
  # Namespace for all the contracts for config validations
  module Contracts
    # Regex to check that topic has a valid format
    TOPIC_REGEXP = /\A(\w|\-|\.)+\z/.freeze
  end
end
