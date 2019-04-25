# frozen_string_literal: true

module WaterDrop
  # Namespace for all the schemas for config validations
  module Schemas
    # Regex to check that topic has a valid format
    TOPIC_REGEXP = /\A(\w|\-|\.)+\z/.freeze
  end
end
