# frozen_string_literal: true

module WaterDrop
  # Namespace used to encapsulate all the internal errors of WaterDrop
  module Errors
    # Base class for all the WaterDrop internal errors
    BaseError = Class.new(StandardError)

    # Raised when configuration doesn't match with validation schema
    InvalidConfiguration = Class.new(BaseError)

    # Raised when we try to send message with invalid options
    InvalidMessageOptions = Class.new(BaseError)

    # Raised when want to hook up to an event that is not registered and supported
    UnregisteredMonitorEvent = Class.new(BaseError)
  end
end
