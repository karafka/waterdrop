# frozen_string_literal: true

module WaterDrop
  module Patches
    module Rdkafka
      # Extends `Rdkafka::Bindings` with some extra methods
      module Bindings
        class << self
          # Add extra methods that we need
          # @param mod [::Rdkafka::Bindings] rdkafka bindings module
          def included(mod)
            mod.attach_function :rd_kafka_name, [:pointer], :string
          end
        end
      end
    end
  end
end

::Rdkafka::Bindings.include(::WaterDrop::Patches::Rdkafka::Bindings)
