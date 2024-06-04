# frozen_string_literal: true

module WaterDrop
  module Instrumentation
    # Namespace for vendor specific instrumentation
    module Vendors
      # Datadog specific instrumentation
      module Datadog
        # Listener that can be used to subscribe to WaterDrop producer to receive stats via StatsD
        # and/or Datadog
        #
        # @note You need to setup the `dogstatsd-ruby` client and assign it
        class MetricsListener
          include ::Karafka::Core::Configurable
          extend Forwardable

          def_delegators :config, :client, :rd_kafka_metrics, :namespace, :default_tags

          # Value object for storing a single rdkafka metric publishing details
          RdKafkaMetric = Struct.new(:type, :scope, :name, :key_location)

          # Namespace under which the DD metrics should be published
          setting :namespace, default: 'waterdrop'

          # Datadog client that we should use to publish the metrics
          setting :client

          # Default tags we want to publish (for example hostname)
          # Format as followed (example for hostname): `["host:#{Socket.gethostname}"]`
          setting :default_tags, default: []

          # All the rdkafka metrics we want to publish
          #
          # By default we publish quite a lot so this can be tuned
          # Note, that the once with `_d` come from WaterDrop, not rdkafka or Kafka
          setting :rd_kafka_metrics, default: [
            # Client metrics
            RdKafkaMetric.new(:count, :root, 'calls', 'tx_d'),
            RdKafkaMetric.new(:histogram, :root, 'queue.size', 'msg_cnt'),

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

          configure

          # @param block [Proc] configuration block
          def initialize(&block)
            configure
            setup(&block) if block
          end

          # @param block [Proc] configuration block
          # @note We define this alias to be consistent with `WaterDrop#setup`
          def setup(&block)
            configure(&block)
          end

          # Hooks up to WaterDrop instrumentation for emitted statistics
          #
          # @param event [Karafka::Core::Monitoring::Event]
          def on_statistics_emitted(event)
            statistics = event[:statistics]

            rd_kafka_metrics.each do |metric|
              report_metric(metric, statistics)
            end
          end

          # Increases the errors count by 1
          #
          # @param _event [Karafka::Core::Monitoring::Event]
          def on_error_occurred(_event)
            count('error_occurred', 1, tags: default_tags)
          end

          # Increases acknowledged messages counter
          # @param _event [Karafka::Core::Monitoring::Event]
          def on_message_acknowledged(_event)
            increment('acknowledged', tags: default_tags)
          end

          %i[
            produced_sync
            produced_async
          ].each do |event_scope|
            class_eval <<~METHODS, __FILE__, __LINE__ + 1
              # @param event [Karafka::Core::Monitoring::Event]
              def on_message_#{event_scope}(event)
                report_message(event[:message][:topic], :#{event_scope})
              end

              # @param event [Karafka::Core::Monitoring::Event]
              def on_messages_#{event_scope}(event)
                event[:messages].each do |message|
                  report_message(message[:topic], :#{event_scope})
                end
              end
            METHODS
          end

          # Reports the buffer usage when anything is added to the buffer
          %i[
            message_buffered
            messages_buffered
          ].each do |event_scope|
            class_eval <<~METHODS, __FILE__, __LINE__ + 1
              # @param event [Karafka::Core::Monitoring::Event]
              def on_#{event_scope}(event)
                histogram(
                  'buffer.size',
                  event[:buffer].size,
                  tags: default_tags
                )
              end
            METHODS
          end

          # Events that support many messages only
          # Reports data flushing operation (production from the buffer)
          %i[
            flushed_sync
            flushed_async
          ].each do |event_scope|
            class_eval <<~METHODS, __FILE__, __LINE__ + 1
              # @param event [Karafka::Core::Monitoring::Event]
              def on_buffer_#{event_scope}(event)
                event[:messages].each do |message|
                  report_message(message[:topic], :#{event_scope})
                end
              end
            METHODS
          end

          private

          %i[
            count
            gauge
            histogram
            increment
            decrement
          ].each do |metric_type|
            class_eval <<~METHODS, __FILE__, __LINE__ + 1
              def #{metric_type}(key, *args)
                client.#{metric_type}(
                  namespaced_metric(key),
                  *args
                )
              end
            METHODS
          end

          # Report that a message has been produced to a topic.
          # @param topic [String] Kafka topic
          # @param method_name [Symbol] method from which this message operation comes
          def report_message(topic, method_name)
            increment(method_name, tags: default_tags + ["topic:#{topic}"])
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
              public_send(
                metric.type,
                metric.name,
                statistics.fetch(*metric.key_location),
                tags: default_tags
              )
            when :brokers
              statistics.fetch('brokers').each_value do |broker_statistics|
                # Skip bootstrap nodes
                # Bootstrap nodes have nodeid -1, other nodes have positive
                # node ids
                next if broker_statistics['nodeid'] == -1

                public_send(
                  metric.type,
                  metric.name,
                  broker_statistics.dig(*metric.key_location),
                  tags: default_tags + ["broker:#{broker_statistics['nodename']}"]
                )
              end
            when :topics
              statistics.fetch('topics').each_value do |topic_statistics|
                public_send(
                  metric.type,
                  metric.name,
                  topic_statistics.dig(*metric.key_location),
                  tags: default_tags + ["topic:#{topic_statistics['topic']}"]
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
