# frozen_string_literal: true

module WaterDrop
  module Instrumentation
    # Namespace for vendor specific instrumentation
    module Vendors
      # Datadog specific instrumentation
      module Datadog
        # Listener that can be used to subscribe to WaterDrop producer to receive stats in DataDog
        #
        # @note You need to setup the `dogstatsd-ruby` client and assign it
        class Listener
          include Dry::Configurable

          # Value object for storing a single rdkafka metric publishing details
          RdKafkaMetric = Struct.new(:type, :scope, :name, :key_location)

          # Namespace under which the DD metrics should be published
          setting :namespace, default: 'waterdrop', reader: true

          # Datadog client that we should use to publish the metrics
          setting :client, reader: true

          # All the rdkafka metrics we want to publish
          #
          # By default we publish quite a lot so this can be tuned
          # Note, that the once with `_d` come from WaterDrop, not rdkafka or Kafka
          setting :rd_kafka_metrics, reader: true, default: [
            # Client metrics
            RdKafkaMetric.new(:count, :root, 'calls', 'tx_d'),
            RdKafkaMetric.new(:histogram, :root, 'queue.size', 'msg_cnt_d'),

            # Broker metrics
            RdKafkaMetric.new(:count, :brokers, 'deliver.attempts', 'txretries_d'),
            RdKafkaMetric.new(:count, :brokers, 'deliver.errors', 'txerrs_d'),
            RdKafkaMetric.new(:count, :brokers, 'receive.errors', 'rxerrs_d'),
            RdKafkaMetric.new(:gauge, :brokers, 'queue.latency.avg', %w[outbuf_latency avg]),
            RdKafkaMetric.new(:gauge, :brokers, 'queue.latency.p95', %w[outbuf_latency p95]),
            RdKafkaMetric.new(:gauge, :brokers, 'queue.latency.p99', %w[outbuf_latency p99]),
            RdKafkaMetric.new(:gauge, :brokers, 'network.latency.avg', %w[rtt avg]),
            RdKafkaMetric.new(:gauge, :brokers, 'network.latency.p95', %w[rtt p95]),
            RdKafkaMetric.new(:gauge, :brokers, 'network.latency.p99', %w[rtt p99])
          ].freeze

          # @param block [Proc] configuration block
          def initialize(&block)
            setup(&block) if block
          end

          # @param block [Proc] configuration block
          # @note We define this alias to be consistent with `WaterDrop#setup`
          def setup(&block)
            configure(&block)
          end

          # Hooks up to WaterDrop instrumentation for emitted statistics
          #
          # @param event [Dry::Events::Event]
          def on_statistics_emitted(event)
            statistics = event[:statistics]

            rd_kafka_metrics.each do |metric|
              report_metric(metric, statistics)
            end
          end

          # Increases the errors count by 1
          #
          # @param _event [Dry::Events::Event]
          def on_error_occurred(_event)
            client.count(
              namespaced_metric('producer.error_occurred'),
              1
            )
          end

          %i[
            produced_sync
            produced_async
            buffered
          ].each do |event_scope|
            class_eval <<~METHODS, __FILE__, __LINE__ + 1
              # @param event [Dry::Events::Event]
              def on_message_#{event_scope}(event)
                report_message(event[:message][:topic], :#{event_scope})
              end

              # @param event [Dry::Events::Event]
              def on_messages_#{event_scope}(event)
                event[:messages].each do |message|
                  report_message(message[:topic], :#{event_scope})
                end
              end
            METHODS
          end

          # Events that support many messages only
          %i[
            flushed_sync
            flushed_async
          ].each do |event_scope|
            class_eval <<~METHODS, __FILE__, __LINE__ + 1
              # @param event [Dry::Events::Event]
              def on_buffer_#{event_scope}(event)
                event[:messages].each do |message|
                  report_message(message[:topic], :#{event_scope})
                end
              end
            METHODS
          end

          private

          # Report that a message has been produced to a topic.
          # @param topic [String] Kafka topic
          # @param method_name [Symbol] method from which this message operation comes
          def report_message(topic, method_name)
            client.increment(
              namespaced_metric("producer.#{method_name}"),
              tags: ["topic:#{topic}"]
            )
          end

          # Wraps metric name in listener's namespace
          # @param metric_name [String] RdKafkaMetric name
          # @return [String]
          def namespaced_metric(metric_name)
            "#{namespace}.#{metric_name}"
          end

          # Reports a given metric statistics to Datadog
          # @param metric [RdKafkaMetric] metric value object
          # @param statistics [Hash] hash with all the statistics emitted
          def report_metric(metric, statistics)
            case metric.scope
            when :root
              client.public_send(
                metric.type,
                namespaced_metric(metric.name),
                statistics.fetch(*metric.key_location)
              )
            when :brokers
              statistics.fetch('brokers').each_value do |broker_statistics|
                # Skip bootstrap nodes
                # Bootstrap nodes have nodeid -1, other nodes have positive
                # node ids
                next if broker_statistics['nodeid'] == -1

                client.public_send(
                  metric.type,
                  namespaced_metric(metric.name),
                  broker_statistics.dig(*metric.key_location),
                  tags: ["broker:#{broker_statistics['nodename']}"]
                )
              end
            else
              raise ArgumentError, metric.scope
            end
          end
        end
      end
    end
  end
end
