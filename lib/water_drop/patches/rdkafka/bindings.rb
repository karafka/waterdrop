# frozen_string_literal: true

module WaterDrop
  module Patches
    module Rdkafka
      # Extends `Rdkafka::Bindings` with some extra methods and updates callbacks that we intend
      # to work with in a bit different way than rdkafka itself
      module Bindings
        class << self
          # Add extra methods that we need
          # @param mod [::Rdkafka::Bindings] rdkafka bindings module
          def included(mod)
            mod.attach_function :rd_kafka_name, [:pointer], :string

            # Default rdkafka setup for errors doest not propagate client details, thus it always
            # publishes all the stuff for all rdkafka instances. We change that by providing
            # function that fetches the instance name, allowing us to have better notifications
            mod.send(:remove_const, :ErrorCallback)
            mod.const_set(:ErrorCallback, build_error_callback)
          end

          # @return [FFI::Function] overwritten callback function
          def build_error_callback
            FFI::Function.new(
              :void, %i[pointer int string pointer]
            ) do |client_prr, err_code, reason, _opaque|
              return nil unless ::Rdkafka::Config.error_callback

              name = ::Rdkafka::Bindings.rd_kafka_name(client_prr)

              error = ::Rdkafka::RdkafkaError.new(err_code, broker_message: reason)

              ::Rdkafka::Config.error_callback.call(name, error)
            end
          end
        end
      end
    end
  end
end

::Rdkafka::Bindings.include(::WaterDrop::Patches::Rdkafka::Bindings)
