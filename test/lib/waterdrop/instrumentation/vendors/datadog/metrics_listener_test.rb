# frozen_string_literal: true

# Base class for Datadog metrics listener tests
class WaterDropInstrumentationVendorsDatadogMetricsListenerBaseTest < WaterDropTest::Base
  private

  def build_dummy_client
    Class.new do
      attr_reader :buffer

      def initialize
        @buffer = Hash.new do |buffer, dd_method|
          buffer[dd_method] = Hash.new do |key_scope, metric|
            key_scope[metric] = []
          end
        end
      end

      %i[
        count
        histogram
        gauge
        increment
        decrement
      ].each do |method_name|
        define_method method_name do |metric, value = nil, details = {}|
          @buffer[method_name][metric] << [value, details]
        end
      end
    end.new
  end

  def build_listener_with_client(dummy_client, default_tags: nil)
    listener = WaterDrop::Instrumentation::Vendors::Datadog::MetricsListener.new

    listener.setup do |config|
      config.client = dummy_client
      config.default_tags = default_tags if default_tags
    end

    listener
  end

  def build_producer_with_listener(listener)
    producer = build(:producer)
    producer.monitor.subscribe(listener)
    producer
  end
end

class WaterDropInstrumentationVendorsDatadogMetricsListenerDefaultTagsTest <
  WaterDropInstrumentationVendorsDatadogMetricsListenerBaseTest
  def setup
    super
    @dummy_client = build_dummy_client
    @listener = build_listener_with_client(@dummy_client, default_tags: %w[test:me])
    @producer = build_producer_with_listener(@listener)
    @topic = "it-#{SecureRandom.uuid}"
  end

  def teardown
    @producer.close
    super
  end

  def test_default_tags_are_included_when_publishing
    @producer.produce_sync(topic: @topic, payload: rand.to_s)

    published_tags = @dummy_client.buffer[:increment]["waterdrop.produced_sync"][0][0][:tags]

    assert_includes published_tags, "test:me"
  end
end

class WaterDropInstrumentationVendorsDatadogMetricsListenerSetupOnInitializationTest <
  WaterDropInstrumentationVendorsDatadogMetricsListenerBaseTest
  def test_setup_upon_initialization_works
    dummy_client = build_dummy_client

    listener = WaterDrop::Instrumentation::Vendors::Datadog::MetricsListener.new do |config|
      config.client = dummy_client
    end

    assert_equal dummy_client, listener.client
  end
end

class WaterDropInstrumentationVendorsDatadogMetricsListenerStatsDispatchTest <
  WaterDropInstrumentationVendorsDatadogMetricsListenerBaseTest
  def setup
    super
    @dummy_client = build_dummy_client
    @listener = build_listener_with_client(@dummy_client)
    @producer = build_producer_with_listener(@listener)
    @topic = "it-#{SecureRandom.uuid}"

    @producer.produce_sync(topic: @topic, payload: rand.to_s)
    sleep(1)
  end

  def teardown
    @producer.close
    super
  end

  def test_proper_metrics_in_place
    counts = @dummy_client.buffer[:count]
    histograms = @dummy_client.buffer[:histogram]
    guages = @dummy_client.buffer[:gauge]
    broker_tag = { tags: %w[broker:127.0.0.1:9092] }

    # count
    assert_includes counts["waterdrop.calls"], [0, { tags: [] }]
    assert_operator counts["waterdrop.calls"].uniq.size, :>, 1
    assert_includes counts["waterdrop.deliver.attempts"], [0, broker_tag]
    assert_includes counts["waterdrop.deliver.errors"], [0, broker_tag]
    assert_includes counts["waterdrop.receive.errors"], [0, broker_tag]

    # histogram
    assert_includes histograms["waterdrop.queue.size"], [0, { tags: [] }]

    # gauge
    assert_operator guages["waterdrop.queue.latency.avg"].uniq.size, :>, 1
    assert_includes guages["waterdrop.queue.latency.avg"], [0, broker_tag]
    assert_operator guages["waterdrop.queue.latency.p95"].uniq.size, :>, 1
    assert_includes guages["waterdrop.queue.latency.p95"], [0, broker_tag]
    assert_operator guages["waterdrop.queue.latency.p99"].uniq.size, :>, 1
    assert_includes guages["waterdrop.queue.latency.p99"], [0, broker_tag]
    assert_operator guages["waterdrop.network.latency.avg"].uniq.size, :>, 1
    assert_includes guages["waterdrop.network.latency.avg"], [0, broker_tag]
    assert_operator guages["waterdrop.network.latency.p95"].uniq.size, :>, 1
    assert_includes guages["waterdrop.network.latency.p95"], [0, broker_tag]
    assert_operator guages["waterdrop.network.latency.p99"].uniq.size, :>, 1
    assert_includes guages["waterdrop.network.latency.p99"], [0, broker_tag]
  end
end

class WaterDropInstrumentationVendorsDatadogMetricsListenerProducingSyncTest <
  WaterDropInstrumentationVendorsDatadogMetricsListenerBaseTest
  def setup
    super
    @dummy_client = build_dummy_client
    @listener = build_listener_with_client(@dummy_client)
    @producer = build_producer_with_listener(@listener)
    @topic = "it-#{SecureRandom.uuid}"

    @producer.produce_sync(topic: @topic, payload: rand.to_s)

    @producer.produce_many_sync(
      [
        { topic: @topic, payload: rand.to_s },
        { topic: @topic, payload: rand.to_s }
      ]
    )
  end

  def teardown
    @producer.close
    super
  end

  def test_proper_metrics_data_in_place
    assert_equal 3, @dummy_client.buffer[:increment]["waterdrop.produced_sync"].size
  end
end

class WaterDropInstrumentationVendorsDatadogMetricsListenerProducingAsyncTest <
  WaterDropInstrumentationVendorsDatadogMetricsListenerBaseTest
  def setup
    super
    @dummy_client = build_dummy_client
    @listener = build_listener_with_client(@dummy_client)
    @producer = build_producer_with_listener(@listener)
    @topic = "it-#{SecureRandom.uuid}"

    @producer.produce_async(topic: @topic, payload: rand.to_s)

    @producer.produce_many_async(
      [
        { topic: @topic, payload: rand.to_s },
        { topic: @topic, payload: rand.to_s }
      ]
    )
  end

  def teardown
    @producer.close
    super
  end

  def test_proper_metrics_data_in_place
    assert_equal 3, @dummy_client.buffer[:increment]["waterdrop.produced_async"].size
  end
end

class WaterDropInstrumentationVendorsDatadogMetricsListenerBufferingTest <
  WaterDropInstrumentationVendorsDatadogMetricsListenerBaseTest
  def setup
    super
    @dummy_client = build_dummy_client
    @listener = build_listener_with_client(@dummy_client)
    @producer = build_producer_with_listener(@listener)
    @topic = "it-#{SecureRandom.uuid}"

    @producer.buffer(topic: @topic, payload: rand.to_s)

    @producer.buffer_many(
      [
        { topic: @topic, payload: rand.to_s },
        { topic: @topic, payload: rand.to_s }
      ]
    )
  end

  def teardown
    @producer.close
    super
  end

  def test_proper_metrics_data_in_place
    assert_equal 2, @dummy_client.buffer[:histogram]["waterdrop.buffer.size"].size
  end
end

class WaterDropInstrumentationVendorsDatadogMetricsListenerFlushingSyncTest <
  WaterDropInstrumentationVendorsDatadogMetricsListenerBaseTest
  def setup
    super
    @dummy_client = build_dummy_client
    @listener = build_listener_with_client(@dummy_client)
    @producer = build_producer_with_listener(@listener)
    @topic = "it-#{SecureRandom.uuid}"

    @producer.buffer(topic: @topic, payload: rand.to_s)
    @producer.flush_sync
  end

  def teardown
    @producer.close
    super
  end

  def test_proper_metrics_data_in_place
    assert_equal 1, @dummy_client.buffer[:increment]["waterdrop.flushed_sync"].size
  end
end

class WaterDropInstrumentationVendorsDatadogMetricsListenerFlushingAsyncTest <
  WaterDropInstrumentationVendorsDatadogMetricsListenerBaseTest
  def setup
    super
    @dummy_client = build_dummy_client
    @listener = build_listener_with_client(@dummy_client)
    @producer = build_producer_with_listener(@listener)
    @topic = "it-#{SecureRandom.uuid}"

    @producer.buffer(topic: @topic, payload: rand.to_s)
    @producer.flush_async
  end

  def teardown
    @producer.close
    super
  end

  def test_proper_metrics_data_in_place
    assert_equal 1, @dummy_client.buffer[:increment]["waterdrop.flushed_async"].size
  end
end

class WaterDropInstrumentationVendorsDatadogMetricsListenerAcknowledgedTest <
  WaterDropInstrumentationVendorsDatadogMetricsListenerBaseTest
  def setup
    super
    @dummy_client = build_dummy_client
    @listener = build_listener_with_client(@dummy_client)
    @producer = build_producer_with_listener(@listener)
    @topic = "it-#{SecureRandom.uuid}"

    @producer.produce_sync(topic: @topic, payload: rand.to_s)
    sleep(0.1)
  end

  def teardown
    @producer.close
    super
  end

  def test_proper_metric_in_place
    assert_equal 1, @dummy_client.buffer[:increment]["waterdrop.acknowledged"].size
  end
end

class WaterDropInstrumentationVendorsDatadogMetricsListenerErrorOccurredTest <
  WaterDropInstrumentationVendorsDatadogMetricsListenerBaseTest
  def setup
    super
    @dummy_client = build_dummy_client
    @listener = build_listener_with_client(@dummy_client)
    @topic = "it-#{SecureRandom.uuid}"

    @producer = build(
      :producer,
      kafka: {
        "bootstrap.servers": "localhost:9093",
        "statistics.interval.ms": 100,
        "message.timeout.ms": 100
      }
    )
    @producer.monitor.subscribe @listener

    @producer.produce_async(topic: @topic, payload: rand.to_s)
  end

  def teardown
    @producer.close
    super
  end

  def test_error_count_increases
    error_received = false
    20.times do
      if @dummy_client.buffer[:count]["waterdrop.error_occurred"].any?
        error_received = true
        break
      end
      sleep(0.25)
    end

    assert_same true, error_received
  end
end

class WaterDropInstrumentationVendorsDatadogMetricsListenerNodeIdNegativeOneTest <
  WaterDropInstrumentationVendorsDatadogMetricsListenerBaseTest
  def test_does_not_publish_metrics_for_node_with_id_negative_one
    dummy_client = build_dummy_client
    listener = build_listener_with_client(dummy_client)

    metric = WaterDrop::Instrumentation::Vendors::Datadog::MetricsListener.new
      .rd_kafka_metrics.last

    statistics = {
      "brokers" => {
        "node_name" => { "nodeid" => -1 }
      }
    }

    listener.send(:report_metric, metric, statistics)
  end
end

class WaterDropInstrumentationVendorsDatadogMetricsListenerNonExistingMetricTest <
  WaterDropInstrumentationVendorsDatadogMetricsListenerBaseTest
  def test_raises_argument_error_for_non_existing_metric
    dummy_client = build_dummy_client
    listener = build_listener_with_client(dummy_client)

    metric = WaterDrop::Instrumentation::Vendors::Datadog::MetricsListener::RdKafkaMetric.new(
      :count, :na, "na"
    )
    statistics = {}

    assert_raises(ArgumentError) do
      listener.send(:report_metric, metric, statistics)
    end
  end
end

class WaterDropInstrumentationVendorsDatadogMetricsListenerTopicLevelMetricTest <
  WaterDropInstrumentationVendorsDatadogMetricsListenerBaseTest
  def setup
    super
    @dummy_client = build_dummy_client
    @listener = build_listener_with_client(@dummy_client)
    @producer = build_producer_with_listener(@listener)
    @topic = "it-#{SecureRandom.uuid}"

    @producer.produce_sync(topic: @topic, payload: rand.to_s)
    sleep(1)

    metric = WaterDrop::Instrumentation::Vendors::Datadog::MetricsListener::RdKafkaMetric.new(
      :gauge, :topics, "topics.batchcnt.avg", %w[batchcnt avg]
    )

    statistics = {
      "name" => "producer-1",
      "brokers" => {},
      "topics" => {
        "test" => {
          "topic" => "test",
          "batchcnt" => {
            "avg" => 6956
          }
        }
      }
    }

    @listener.send(:report_metric, metric, statistics)
  end

  def teardown
    @producer.close
    super
  end

  def test_report_metric_publishes_statistic_with_topic_tag
    guages = @dummy_client.buffer[:gauge]

    assert_equal [[6956, { tags: ["topic:test"] }]],
      guages["waterdrop.topics.batchcnt.avg"]
  end
end
