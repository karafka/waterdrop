# frozen_string_literal: true

# This module provides compatibility methods to mimic Factory Bot's API
# while using regular factory methods.
module Factories
  # Mimics FactoryBot.build by calling the appropriate factory method
  # @param factory_name [Symbol, String]
  # @param attributes [Hash]
  def build(factory_name, **attributes)
    factory_method = "#{factory_name}_factory"

    unless respond_to?(factory_method)
      raise(
        NoMethodError,
        "Factory method '#{factory_method}' not found factory '#{factory_name}'"
      )
    end

    public_send(factory_method, **attributes)
  end
end
