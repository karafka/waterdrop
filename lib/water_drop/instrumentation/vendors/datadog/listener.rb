# frozen_string_literal: true

module WaterDrop
  module Instrumentation
    module Vendors
      module Datadog
        # Listener that can be used to subscribe to WaterDrop producer to receive stats in DataDog
        #
        # @note You need to setup the datadog/statsd client and assign it
        class Listener
          include Dry::Configurable

          # Value object for storing a single rdkafka metric publishing details
          RdkafkaMetric = Struct.new(:type, :scope, :name, :key_location)

          # Namespace under which the DD metrics should be published
          setting :namespace, default: 'waterdrop', reader: true

          # Datadog client that we should use to publish the metrics
          setting :client, reader: true

          # All the rdkafka metrics we want to publish
          #
          # By default we publish quite a lot so this can be tuned
          # Note, that the once with `_d` come from WaterDrop, not rdkafka or Kafka
          setting :rdkafka_metrics, reader: true, default: [
            # Client metrics
            RdkafkaMetric.new(:count, :root, 'calls', 'tx_d'),
            RdkafkaMetric.new(:histogram, :root, 'queue.size', 'msg_cnt_d'),

            # Broker metrics
            RdkafkaMetric.new(:count, :brokers, 'deliver.attempts', 'txretries_d'),
            RdkafkaMetric.new(:count, :brokers, 'deliver.errors', 'txerrs_d'),
            RdkafkaMetric.new(:count, :brokers, 'receive.errors', 'rxerrs_d'),
            RdkafkaMetric.new(:gauge, :brokers, 'queue.latency.avg', %w[outbuf_latency avg]),
            RdkafkaMetric.new(:gauge, :brokers, 'queue.latency.p95', %w[outbuf_latency p95]),
            RdkafkaMetric.new(:gauge, :brokers, 'queue.latency.p99', %w[outbuf_latency p99]),
            RdkafkaMetric.new(:gauge, :brokers, 'network.latency.avg', %w[rtt avg]),
            RdkafkaMetric.new(:gauge, :brokers, 'network.latency.p95', %w[rtt p95]),
            RdkafkaMetric.new(:gauge, :brokers, 'network.latency.p99', %w[rtt p99])
          ].freeze

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

            rdkafka_metrics.each do |metric|
              report_metric(metric, statistics)
            end
          end

          %i[
            produced_sync
            produced_async
            buffered
          ].each do |event_scope|
            class_eval <<~METHODS
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
            class_eval <<~METHODS
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
          def report_message(topic, type)
            client.increment(
              namespaced_metric("producer.#{type}"),
              tags: ["topic:#{topic}"]
            )
          end

          # Wraps metric name in listener's namespace
          # @param metric_name [String] RdkafkaMetric name
          # @return [String]
          def namespaced_metric(metric_name)
            "#{namespace}.#{metric_name}"
          end

          # Reports a given metric statistics to Datadog
          # @param metric [RdkafkaMetric] metric value object
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
              statistics
                .fetch('brokers')
                .values
                .each do |broker_statistics|
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
