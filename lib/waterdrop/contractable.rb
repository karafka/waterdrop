# frozen_string_literal: true

module WaterDrop
  # Contract layer for WaterDrop and Karafka
  # It aims to be "dry-validation" like but smaller and easier to handle + without dependencies
  #
  # It allows for nested validations, etc
  #
  # @note It is thread-safe to run but validations definitions should happen before threads are
  #   used.
  module Contractable
  end
end
