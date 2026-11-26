# frozen_string_literal: true

module WaterDrop
  module Instrumentation
    module Vendors
      # OpenTelemetry specific instrumentation
      module OpenTelemetry
        # Producer middleware that injects the W3C trace context into the message headers and
        # creates a producer span for each message
        #
        # @note You need to setup the `opentelemetry-sdk` (or `opentelemetry-api`). Without an
        #   assigned tracer, the `waterdrop` tracer of the global tracer provider is used
        #
        # @note The span starts and finishes in the middleware, before the message is validated and
        #   dispatched. It does not cover the delivery to Kafka.
        #
        # @note Middleware runs for buffered messages when they are flushed, so they get the trace
        #   context of the `flush_*` caller, not of the `buffer` caller.
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

          # OpenTelemetry tracer that we should use to create the producer spans. When `nil`, the
          # `waterdrop` tracer of the global `OpenTelemetry.tracer_provider` is used
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
            @default_tracer = nil
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
          # @note Calling it again does not append the middleware twice. Call it during setup,
          #   not while producers are being configured.
          def install
            ::WaterDrop.monitor.unsubscribe(self)
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

            return message unless injectable

            context = if span
              ::OpenTelemetry::Trace.context_with_span(span)
            else
              ::OpenTelemetry::Context.current
            end

            carrier = {}
            (propagator || ::OpenTelemetry.propagation).inject(carrier, context: context)

            return message if carrier.empty?

            message = message.dup
            # Merged into the carrier to skip a second Hash; injected values win
            message[:headers] = headers ? carrier.merge!(headers) { |_, injected, _| injected } : carrier
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

            # Binary keys are not valid attribute values and exporters drop them
            if key.is_a?(String) && (key.ascii_only? || (key.encoding == Encoding::UTF_8 && key.valid_encoding?))
              attributes["kafka.message.key"] = key
            end

            (tracer || default_tracer).start_span("send #{topic}", kind: :producer, attributes: attributes)
          end

          # @return [OpenTelemetry::Trace::Tracer] tracer of the global tracer provider
          def default_tracer
            @default_tracer ||= ::OpenTelemetry.tracer_provider.tracer("waterdrop", ::WaterDrop::VERSION)
          end
        end
      end
    end
  end
end
