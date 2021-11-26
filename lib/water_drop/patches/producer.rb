# frozen_string_literal: true

module WaterDrop
  module Patches
    module Producer
      def rd_kafka_name
        Rdkafka::Bindings.rd_kafka_name @native_kafka
      end
    end
  end
end

Rdkafka::Producer.include ::WaterDrop::Patches::Producer
