# frozen_string_literal: true

module WaterDrop
  module Patches
    module Bindings
      class << self
        def included(mod)
          mod.attach_function :rd_kafka_name, [:pointer], :string
        end
      end
    end
  end
end

Rdkafka::Bindings.include WaterDrop::Patches::Bindings
