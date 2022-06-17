# frozen_string_literal: true

module WaterDrop
  # Patches to external components
  module Patches
    # Rdkafka related patches
    module Rdkafka
      # Rdkafka::Producer patches
      module Producer
        # Adds a method that allows us to get the native kafka producer name
        # @return [String] producer instance name
        def name
          @_gem_version ||= ::Gem::Version.new(::Rdkafka::VERSION)
          # 0.12.0 changed how the native producer client reference works.
          # This code supports both older and newer versions of rdkafka
          @_native ||=  @_gem_version >= '0.12.0' ? @client.native : @native_kafka

          ::Rdkafka::Bindings.rd_kafka_name(@_native)
        end
      end
    end
  end
end

::Rdkafka::Producer.include ::WaterDrop::Patches::Rdkafka::Producer
