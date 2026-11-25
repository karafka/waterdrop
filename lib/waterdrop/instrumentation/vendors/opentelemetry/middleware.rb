# frozen_string_literal: true

module WaterDrop
  module Instrumentation
    module Vendors
      # OpenTelemetry specific instrumentation
      module OpenTelemetry
        # Producer middleware that injects the W3C trace context into the message headers and
        # creates a producer span for each message
        #
        # @note You need to setup the `opentelemetry-sdk` (or `opentelemetry-api`) and assign the
        #   tracer
        #
        # @note The span starts and finishes in the middleware, before the message is validated and
        #   dispatched. It does not cover the delivery to Kafka.
        #
        # @example Append to one producer
        #   middleware = WaterDrop::Instrumentation::Vendors::OpenTelemetry::Middleware.new do |c|
        #     c.tracer = OpenTelemetry.tracer_provider.tracer("waterdrop")
        #   end
        #
        #   producer.middleware.append(middleware)
        #
        # @example Append to every producer configured from now on
        #   middleware.install
        class Middleware
          include ::Karafka::Core::Configurable
          extend Forwardable

          def_delegators :config, :tracer, :propagator, :spans

          # W3C trace context header name
          TRACEPARENT = "traceparent"

          # Attributes shared by all the producer spans
          BASE_ATTRIBUTES = {
            "messaging.system.name" => "kafka",
            "messaging.operation.name" => "send",
            "messaging.operation.type" => "send"
          }.freeze

          private_constant :TRACEPARENT, :BASE_ATTRIBUTES

          # OpenTelemetry tracer that we should use to create the producer spans
          setting :tracer

          # Propagator used to inject the trace context. When `nil`, the global
          # `OpenTelemetry.propagation` is used at the time of producing
          setting :propagator

          # Should we create a producer span for each message. When `false`, only the current trace
          # context is injected, which is much cheaper on `produce_many_*` with many messages
          setting :spans, default: true

          configure

          # @param block [Proc] configuration block
          def initialize(&block)
            configure
            setup(&block) if block
          end

          # @note We define this alias to be consistent with `WaterDrop#setup`
          def setup(...)
            configure(...)
          end

          # Appends this middleware to every producer that gets configured after this call
          #
          # @note Producers configured before this call are not affected. Append to them directly.
          # @note Use `WaterDrop.monitor.unsubscribe(middleware)` to stop it
          def install
            ::WaterDrop.monitor.subscribe(self)
          end

          # @param event [Karafka::Core::Monitoring::Event]
          def on_producer_configured(event)
            event[:producer].middleware.append(self)
          end

          # @param message [Hash] message hash
          # @return [Hash] copy of the message with the trace context in its headers, or the same
          #   message when there is nothing to inject
          def call(message)
            headers = message[:headers]

            # Invalid headers are left for the message contract to reject
            return message unless headers.nil? || headers.is_a?(Hash)

            # A trace context set by the user is never overwritten
            injectable = headers.nil? || !headers.key?(TRACEPARENT)

            return message unless spans || injectable

            span = start_span(message) if spans

            if injectable
              message = message.dup
              headers = headers ? headers.dup : {}
              message[:headers] = headers

              context = if span
                ::OpenTelemetry::Trace.context_with_span(span)
              else
                ::OpenTelemetry::Context.current
              end

              (propagator || ::OpenTelemetry.propagation).inject(headers, context: context)
            end

            message
          ensure
            span&.finish
          end

          private

          # @param message [Hash]
          # @return [OpenTelemetry::Trace::Span] started producer span
          def start_span(message)
            topic = message[:topic].to_s
            attributes = BASE_ATTRIBUTES.merge("messaging.destination.name" => topic)

            partition = message[:partition]

            if partition.is_a?(Integer) && partition >= 0
              attributes["messaging.destination.partition.id"] = partition.to_s
            end

            key = message[:key]
            attributes["kafka.message.key"] = key if key

            tracer.start_span("send #{topic}", kind: :producer, attributes: attributes)
          end
        end
      end
    end
  end
end
