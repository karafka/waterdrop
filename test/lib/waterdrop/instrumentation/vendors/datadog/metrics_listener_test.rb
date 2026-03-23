# frozen_string_literal: true

describe_current do
  before do
    @dummy_client = Class.new do
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

    @listener = described_class.new
    @listener.setup do |config|
      config.client = @dummy_client
    end

    @topic = generate_topic

    @producer = build(:producer)
    @producer.monitor.subscribe @listener
  end

  after { @producer.close }

  describe "when having some default tags present" do
    before do
      @listener = described_class.new
      @listener.setup do |config|
        config.client = @dummy_client
        config.default_tags = %w[test:me]
      end

      @producer = build(:producer)
      @producer.monitor.subscribe @listener
    end

    describe "when publishing, default tags should be included" do
      before { @producer.produce_sync(topic: @topic, payload: rand.to_s) }

      it "expect to have proper metrics data in place" do
        published_tags = @dummy_client.buffer[:increment]["waterdrop.produced_sync"][0][0][:tags]

        assert_includes(published_tags, "test:me")
      end
    end
  end

  describe "when setting up a listener upon initialization" do
    before do
      @listener = described_class.new do |config|
        config.client = @dummy_client
      end
    end

    it "expect to work" do
      assert_equal(@dummy_client, @listener.client)
    end
  end

  # Here we focus on metrics coming from rdkafka, we dispatch this single message just to have
  # some fluctuation
  describe "when expecting emitted stats DD dispatch" do
    # Just to trigger some stats
    before do
      @producer.produce_sync(topic: @topic, payload: rand.to_s)

      # Give it some time to emit the stats
      sleep(1)

      @counts = @dummy_client.buffer[:count]
      @histograms = @dummy_client.buffer[:histogram]
      @guages = @dummy_client.buffer[:gauge]
      @broker_tag = { tags: %w[broker:127.0.0.1:9092] }
    end

    # We add all expectations in one example not to sleep each time
    it "expect to have proper metrics in place" do
      # count
      assert_includes(@counts["waterdrop.calls"], [0, { tags: [] }])
      assert_operator(@counts["waterdrop.calls"].uniq.size, :>, 1)
      assert_includes(@counts["waterdrop.deliver.attempts"], [0, @broker_tag])
      assert_includes(@counts["waterdrop.deliver.errors"], [0, @broker_tag])
      assert_includes(@counts["waterdrop.receive.errors"], [0, @broker_tag])

      # histogram
      assert_includes(@histograms["waterdrop.queue.size"], [0, { tags: [] }])
      assert_includes(@histograms["waterdrop.queue.size"], [0, { tags: [] }])

      # gauge
      assert_operator(@guages["waterdrop.queue.latency.avg"].uniq.size, :>, 1)
      assert_includes(@guages["waterdrop.queue.latency.avg"], [0, @broker_tag])
      assert_operator(@guages["waterdrop.queue.latency.p95"].uniq.size, :>, 1)
      assert_includes(@guages["waterdrop.queue.latency.p95"], [0, @broker_tag])
      assert_operator(@guages["waterdrop.queue.latency.p99"].uniq.size, :>, 1)
      assert_includes(@guages["waterdrop.queue.latency.p99"], [0, @broker_tag])
      assert_operator(@guages["waterdrop.network.latency.avg"].uniq.size, :>, 1)
      assert_includes(@guages["waterdrop.network.latency.avg"], [0, @broker_tag])
      assert_operator(@guages["waterdrop.network.latency.p95"].uniq.size, :>, 1)
      assert_includes(@guages["waterdrop.network.latency.p95"], [0, @broker_tag])
      assert_operator(@guages["waterdrop.network.latency.p99"].uniq.size, :>, 1)
      assert_includes(@guages["waterdrop.network.latency.p99"], [0, @broker_tag])
    end
  end

  describe "when producing sync" do
    before do
      @producer.produce_sync(topic: @topic, payload: rand.to_s)

      @producer.produce_many_sync(
        [
          { topic: @topic, payload: rand.to_s },
          { topic: @topic, payload: rand.to_s }
        ]
      )
    end

    it "expect to have proper metrics data in place" do
      assert_equal(3, @dummy_client.buffer[:increment]["waterdrop.produced_sync"].size)
    end
  end

  describe "when producing async" do
    before do
      @producer.produce_async(topic: @topic, payload: rand.to_s)

      @producer.produce_many_async(
        [
          { topic: @topic, payload: rand.to_s },
          { topic: @topic, payload: rand.to_s }
        ]
      )
    end

    it "expect to have proper metrics data in place" do
      assert_equal(3, @dummy_client.buffer[:increment]["waterdrop.produced_async"].size)
    end
  end

  describe "when buffering" do
    before do
      @producer.buffer(topic: @topic, payload: rand.to_s)

      @producer.buffer_many(
        [
          { topic: @topic, payload: rand.to_s },
          { topic: @topic, payload: rand.to_s }
        ]
      )
    end

    it "expect to have proper metrics data in place" do
      assert_equal(2, @dummy_client.buffer[:histogram]["waterdrop.buffer.size"].size)
    end
  end

  describe "when flushing sync" do
    before do
      @producer.buffer(topic: @topic, payload: rand.to_s)
      @producer.flush_sync
    end

    it "expect to have proper metrics data in place" do
      assert_equal(1, @dummy_client.buffer[:increment]["waterdrop.flushed_sync"].size)
    end
  end

  describe "when flushing async" do
    before do
      @producer.buffer(topic: @topic, payload: rand.to_s)
      @producer.flush_async
    end

    it "expect to have proper metrics data in place" do
      assert_equal(1, @dummy_client.buffer[:increment]["waterdrop.flushed_async"].size)
    end
  end

  describe "when message is acknowledged" do
    before do
      @producer.produce_sync(topic: @topic, payload: rand.to_s)
      # We need to give the async callback a bit of time to kick in
      sleep(0.1)
    end

    it "expect to have a proper metric in place" do
      assert_equal(1, @dummy_client.buffer[:increment]["waterdrop.acknowledged"].size)
    end
  end

  describe "when error occurred" do
    before do
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

    it "expect error count to increase" do
      # Wait for error callback with retries since it depends on librdkafka timing
      error_received = false
      20.times do
        if @dummy_client.buffer[:count]["waterdrop.error_occurred"].any?
          error_received = true
          break
        end
        sleep(0.25)
      end

      assert(error_received)
    end
  end

  describe "when we encounter a node with id -1" do
    before do
      @metric = described_class.new.rd_kafka_metrics.last
      @statistics = {
        "brokers" => {
          "node_name" => { "nodeid" => -1 }
        }
      }
    end

    it "expect not to publish metrics on it" do
      @listener.send(:report_metric, @metric, @statistics)
    end
  end

  describe "when we try to publish a non-existing metric" do
    before do
      @metric = described_class::RdKafkaMetric.new(:count, :na, "na")
      @statistics = {}
    end

    it do
      assert_raises(ArgumentError) { @listener.send(:report_metric, @metric, @statistics) }
    end
  end

  describe "when trying to publish a topic level metric" do
    before do
      @producer.produce_sync(topic: @topic, payload: rand.to_s)
      sleep(1)

      @guages = @dummy_client.buffer[:gauge]
      @metric = described_class::RdKafkaMetric.new(:gauge, :topics, "topics.batchcnt.avg", %w[batchcnt avg])
      @statistics = {
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

      @listener.send(:report_metric, @metric, @statistics)
    end

    it "report metric will publish statistic with topic tag" do
      assert_equal([[6956, { tags: ["topic:test"] }]], @guages["waterdrop.topics.batchcnt.avg"])
    end
  end
end
