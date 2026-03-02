# frozen_string_literal: true

require "test_helper"

describe WaterDrop::Producer::Buffer do
  before do
    @producer = build(:producer)
    @invalid_error = WaterDrop::Errors::MessageInvalidError
  end

  after do
    @producer.purge
    @producer.close
  end

  describe "#buffer" do
    context "when producer is closed" do
      before { @producer.close }

      it do
        @message = build(:valid_message)
        assert_raises(WaterDrop::Errors::ProducerClosedError) { @producer.buffer(@message) }
      end
    end

    context "when message is invalid" do
      before do
        @message = build(:invalid_message)
      end

      it { @producer.buffer(@message) }

      it "expect to raise on attempt to flush" do
        @producer.buffer(@message)
        assert_raises(@invalid_error) { @producer.flush_async }
      end
    end

    context "when message is valid" do
      before do
        @message = build(:valid_message)
      end

      it { assert_includes(@producer.buffer(@message), @message) }
    end

    context "with middleware" do
      before do
        @message = build(:valid_message)

        @middleware = lambda do |message|
          message[:payload] += "test "
          message
        end

        @producer.middleware.append(@middleware)
      end

      it "expect to run middleware only once during the flow" do
        @producer.buffer(@message)
        @producer.flush_async
        assert_equal(1, @message[:payload].scan("test").size)
      end
    end
  end

  describe "#buffer_many" do
    context "when producer is closed" do
      before { @producer.close }

      it do
        @messages = [build(:valid_message)]
        assert_raises(WaterDrop::Errors::ProducerClosedError) { @producer.buffer_many(@messages) }
      end
    end

    context "when we have several invalid messages" do
      before do
        @messages = Array.new(10) { build(:invalid_message) }
      end

      it { @producer.buffer_many(@messages) }

      it "expect to validate on flush" do
        @producer.buffer_many(@messages)
        assert_raises(@invalid_error) { @producer.flush_async }
      end
    end

    context "when the last message out of a batch is invalid" do
      before do
        @messages = [build(:valid_message), build(:invalid_message)]
      end

      it { @producer.buffer_many(@messages) }

      it "expect to validate on flush" do
        @producer.buffer_many(@messages)
        assert_raises(@invalid_error) { @producer.flush_async }
      end
    end

    context "when we have several valid messages" do
      before do
        @messages = Array.new(10) { build(:valid_message) }
      end

      it "expect all the results to be buffered" do
        assert_equal(@messages, @producer.buffer_many(@messages))
      end
    end

    context "with middleware" do
      before do
        @message = build(:valid_message)

        @middleware = lambda do |message|
          message[:payload] += "test "
          message
        end

        @producer.middleware.append(@middleware)
      end

      it "expect to run middleware only once during the flow" do
        @producer.buffer_many([@message])
        @producer.flush_async
        assert_equal(1, @message[:payload].scan("test").size)
      end
    end
  end

  describe "#flush_async" do
    context "when there are no messages in the buffer" do
      it { assert_equal([], @producer.flush_async) }
    end

    context "when there are messages in the buffer" do
      before { @producer.buffer(build(:valid_message)) }

      it { assert_kind_of(Rdkafka::Producer::DeliveryHandle, @producer.flush_async[0]) }
      it { assert_empty(@producer.tap(&:flush_async).messages) }
    end

    context "when an error occurred during flushing" do
      before do
        @error = Rdkafka::RdkafkaError.new(0)
      end

      it do
        @producer.client.stub(:produce, ->(*) { raise @error }) do
          @producer.buffer(build(:valid_message))
          assert_raises(WaterDrop::Errors::ProduceManyError) { @producer.flush_async }
        end
      end
    end
  end

  describe "#flush_sync" do
    context "when there are no messages in the buffer" do
      it { assert_equal([], @producer.flush_sync) }
    end

    context "when there are messages in the buffer" do
      before { @producer.buffer(build(:valid_message)) }

      it { assert_kind_of(Rdkafka::Producer::DeliveryHandle, @producer.flush_sync[0]) }
      it { assert_empty(@producer.tap(&:flush_sync).messages) }
    end

    context "when an error occurred during flushing" do
      before do
        @error = Rdkafka::RdkafkaError.new(0)
      end

      it do
        @producer.client.stub(:produce, ->(*) { raise @error }) do
          @producer.buffer(build(:valid_message))
          assert_raises(WaterDrop::Errors::ProduceManyError) { @producer.flush_sync }
        end
      end
    end
  end

  context "when we have data in the buffer" do
    before { @producer.buffer(build(:valid_message)) }

    it "expect not to allow for a disconnect" do
      assert_equal(false, @producer.disconnect)
    end
  end
end
