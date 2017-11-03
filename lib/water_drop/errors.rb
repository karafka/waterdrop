# frozen_string_literal: true

module WaterDrop
  # Namespace used to encapsulate all the internal errors of WaterDrop
  module Errors
    # Base class for all the WaterDrop internal errors
    BaseError = Class.new(StandardError)

    # Raised when configuration doesn't match with validation schema
    InvalidConfiguration = Class.new(BaseError)

    # Raised when we try to send message with invalid optionss
    InvalidMessageOptions = Class.new(BaseError)
  end
end
