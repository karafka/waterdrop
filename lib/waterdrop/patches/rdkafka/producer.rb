# frozen_string_literal: true

module WaterDrop
  # Patches to external components
  module Patches
    # Rdkafka related patches
    module Rdkafka
      # Rdkafka::Producer patches
      module Producer
        # Adds a method that allows us to get the native kafka producer name
        #
        # In between rdkafka versions, there are internal changes that force us to add some extra
        # magic to support all the versions.
        #
        # @return [String] producer instance name
        def name
          unless @_native
            version = ::Gem::Version.new(::Rdkafka::VERSION)

            if version < ::Gem::Version.new('0.12.0')
              @native = @native_kafka
            elsif version < ::Gem::Version.new('0.13.0.beta.1')
              @_native = @client.native
            else
              @_native = @native_kafka.inner
            end
          end

          ::Rdkafka::Bindings.rd_kafka_name(@_native)
        end
      end
    end
  end
end

::Rdkafka::Producer.include ::WaterDrop::Patches::Rdkafka::Producer
