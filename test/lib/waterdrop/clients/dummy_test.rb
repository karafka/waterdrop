# frozen_string_literal: true

require "test_helper"

describe_current do
  before do
    @client = described_class.new(nil)
    @producer = WaterDrop::Producer.new do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end
  end

  after { @producer.close }

  describe "publishing interface" do
    it "expect to return self for further chaining" do
      assert_equal(@client, @client.publish_sync({}))
    end

    context "when using producer #produce_async" do
      before do
        @handler = @producer.produce_async(topic: "test", partition: 2, payload: "1")
      end

      it { assert_kind_of(described_class::Handle, @handler) }
      it { assert_equal("test", @handler.wait.topic_name) }
      it { assert_equal(2, @handler.wait.partition) }
      it { assert_equal(0, @handler.wait.offset) }

      context "with multiple dispatches to the same topic partition" do
        before do
          @last_handler = nil
          3.times { @last_handler = @producer.produce_async(topic: "test", partition: 2, payload: "1") }
        end

        it { assert_kind_of(described_class::Handle, @last_handler) }
        it { assert_equal("test", @last_handler.wait.topic_name) }
        it { assert_equal(2, @last_handler.wait.partition) }
        it { assert_equal(3, @last_handler.wait.offset) }
      end

      context "with multiple dispatches to different partitions" do
        before do
          @last_handler = nil
          3.times { |i| @last_handler = @producer.produce_async(topic: "test", partition: i, payload: "1") }
        end

        it { assert_kind_of(described_class::Handle, @last_handler) }
        it { assert_equal("test", @last_handler.wait.topic_name) }
        it { assert_equal(2, @last_handler.wait.partition) }
        it { assert_equal(1, @last_handler.wait.offset) }
      end

      context "with multiple dispatches to different topics" do
        before do
          3.times { |i| @producer.produce_async(topic: "test#{i}", partition: 0, payload: "1") }
        end

        it { assert_kind_of(described_class::Handle, @handler) }
        it { assert_equal("test", @handler.wait.topic_name) }
        it { assert_equal(2, @handler.wait.partition) }
        it { assert_equal(0, @handler.wait.offset) }
      end
    end

    context "when using producer #produce_sync" do
      before do
        @report = @producer.produce_sync(topic: "test", partition: 2, payload: "1")
      end

      it { assert_kind_of(Rdkafka::Producer::DeliveryReport, @report) }
      it { assert_equal("test", @report.topic_name) }
      it { assert_equal(2, @report.partition) }
      it { assert_equal(0, @report.offset) }

      context "with multiple dispatches to the same topic partition" do
        before do
          @last_report = nil
          3.times { @last_report = @producer.produce_sync(topic: "test", partition: 2, payload: "1") }
        end

        it { assert_kind_of(Rdkafka::Producer::DeliveryReport, @last_report) }
        it { assert_equal("test", @last_report.topic_name) }
        it { assert_equal(2, @last_report.partition) }
        it { assert_equal(3, @last_report.offset) }
      end

      context "with multiple dispatches to different partitions" do
        before do
          @last_report = nil
          3.times { |i| @last_report = @producer.produce_sync(topic: "test", partition: i, payload: "1") }
        end

        it { assert_kind_of(Rdkafka::Producer::DeliveryReport, @last_report) }
        it { assert_equal("test", @last_report.topic_name) }
        it { assert_equal(2, @last_report.partition) }
        it { assert_equal(1, @last_report.offset) }
      end

      context "with multiple dispatches to different topics" do
        before do
          3.times { |i| @producer.produce_sync(topic: "test#{i}", partition: 0, payload: "1") }
        end

        it { assert_kind_of(Rdkafka::Producer::DeliveryReport, @report) }
        it { assert_equal("test", @report.topic_name) }
        it { assert_equal(2, @report.partition) }
        it { assert_equal(0, @report.offset) }
      end
    end
  end

  describe "#respond_to?" do
    it { assert_respond_to(@client, :test) }
  end

  describe "#queue_size" do
    it { assert_equal(0, @client.queue_size) }
    it { assert_equal(0, @client.queue_length) }
  end

  describe "#transaction" do
    before do
      @prev_producer = @producer
      @producer = WaterDrop::Producer.new do |config|
        config.deliver = false
        config.kafka = {
          "bootstrap.servers": BOOTSTRAP_SERVERS,
          "transactional.id": SecureRandom.uuid
        }
      end
    end

    after { @prev_producer&.close }

    context "when no error and no abort" do
      it "expect to return the block value" do
        assert_equal(1, @producer.transaction { 1 })
      end
    end

    context "when WaterDrop::AbortTransaction occurs" do
      it "expect not to raise error" do
        @producer.transaction { raise(WaterDrop::AbortTransaction) }
      end
    end

    context "when different error occurs" do
      it "expect to raise error" do
        assert_raises(StandardError) do
          @producer.transaction { raise(StandardError) }
        end
      end
    end

    context "when running a nested transaction" do
      it "expect to work ok" do
        result = @producer.transaction do
          @producer.transaction do
            @producer.produce_sync(topic: "1", payload: "2")
            2
          end
        end

        assert_equal(2, result)
      end
    end
  end
end
