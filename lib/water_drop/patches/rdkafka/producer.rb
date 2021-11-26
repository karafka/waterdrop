# frozen_string_literal: true

module WaterDrop
  # Patches to external components
  module Patches
    # Rdkafka related patches
    module Rdkafka
      # Rdkafka::Producer patches
      module Producer
        # Adds a method that allows us to get the nativ kafka producer name
        # @return [String] producer instance name
        def name
          ::Rdkafka::Bindings.rd_kafka_name(@native_kafka)
        end
      end
    end
  end
end

::Rdkafka::Producer.include ::WaterDrop::Patches::Rdkafka::Producer
