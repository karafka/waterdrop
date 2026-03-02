# frozen_string_literal: true

describe_current do
  before do
    @buffered_message_stub = Struct.new(:topic, :partition, :offset, keyword_init: true)
    @buffered_consumer_stub = Struct.new(:consumer_group_metadata_pointer, keyword_init: true)
    @topic_name = "it-#{SecureRandom.uuid}"
    @producer = WaterDrop::Producer.new do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end
    @all_messages = [
      { payload: "one", topic: "foo" },
      { payload: "one", topic: "bar" },
      { payload: "two", topic: "foo" }
    ]
    @foo_messages = [
      { payload: "one", topic: "foo" },
      { payload: "two", topic: "foo" }
    ]
    @bar_messages = [
      { payload: "one", topic: "bar" }
    ]
    @client = described_class.new(@producer)
    @producer.stub(:client, @client) do
      @producer.produce_sync(payload: "one", topic: "foo")
      @producer.produce_sync(payload: "one", topic: "bar")
      @producer.produce_sync(payload: "two", topic: "foo")
    end
  end

  after { @producer.close }

  describe "#messages" do
    it { assert_equal(@all_messages, @client.messages) }
  end

  describe "#messages_for" do
    context "with topic that has messages produced to it" do
      it { assert_equal(@foo_messages, @client.messages_for("foo")) }
      it { assert_equal(@bar_messages, @client.messages_for("bar")) }
    end

    context "with topic that has no messages produced to it" do
      it { assert_empty(@client.messages_for("buzz")) }
    end
  end

  describe "#reset" do
    before { @client.reset }

    it { assert_empty(@client.messages) }
    it { assert_empty(@client.messages_for("foo")) }
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
      @client = described_class.new(@producer)
    end

    after { @prev_producer&.close }

    context "when no error and no abort" do
      it "expect to return the block value" do
        @producer.stub(:client, @client) do
          assert_equal(1, @producer.transaction { 1 })
        end
      end
    end

    context "when running transaction with production of messages" do
      it "expect to add them to the buffers" do
        @producer.stub(:client, @client) do
          @producer.transaction do
            @producer.produce_sync(topic: @topic_name, payload: "test")
            @producer.produce_sync(topic: @topic_name, payload: "test")
          end

          assert_equal(2, @client.messages.size)
          assert_equal(2, @client.messages_for(@topic_name).size)
        end
      end
    end

    context "when running nested transaction with production of messages" do
      it "expect to add them to the buffers" do
        @producer.stub(:client, @client) do
          @producer.transaction do
            @producer.produce_sync(topic: @topic_name, payload: "test")
            @producer.produce_sync(topic: @topic_name, payload: "test")

            @producer.transaction do
              @producer.produce_sync(topic: @topic_name, payload: "test")
              @producer.produce_sync(topic: @topic_name, payload: "test")
            end
          end

          assert_equal(4, @client.messages.size)
          assert_equal(4, @client.messages_for(@topic_name).size)
        end
      end
    end

    context "when running nested transaction with production of messages on abort" do
      it "expect to add them to the buffers" do
        @producer.stub(:client, @client) do
          @producer.transaction do
            @producer.produce_sync(topic: @topic_name, payload: "test")
            @producer.produce_sync(topic: @topic_name, payload: "test")

            @producer.transaction do
              @producer.produce_sync(topic: @topic_name, payload: "test")
              @producer.produce_sync(topic: @topic_name, payload: "test")

              raise WaterDrop::AbortTransaction
            end
          end

          assert_equal(0, @client.messages.size)
          assert_equal(0, @client.messages_for("test").size)
        end
      end
    end

    context "when abort occurs" do
      it "expect not to raise error" do
        @producer.stub(:client, @client) do
          @producer.transaction { raise WaterDrop::AbortTransaction }
        end
      end

      it "expect not to contain messages from the aborted transaction" do
        @producer.stub(:client, @client) do
          @producer.transaction do
            @producer.produce_sync(topic: @topic_name, payload: "test")

            raise WaterDrop::AbortTransaction
          end

          assert_equal(0, @client.messages.size)
          assert_empty(@client.messages_for("test"))
        end
      end
    end

    context "when WaterDrop::AbortTransaction error occurs" do
      it "expect not to raise error" do
        @producer.stub(:client, @client) do
          @producer.transaction { raise(WaterDrop::AbortTransaction) }
        end
      end
    end

    context "when different error occurs" do
      it "expect to raise error" do
        @producer.stub(:client, @client) do
          assert_raises(StandardError) do
            @producer.transaction { raise(StandardError) }
          end
        end
      end

      it "expect not to contain messages from the aborted transaction" do
        @producer.stub(:client, @client) do
          assert_raises(StandardError) do
            @producer.transaction do
              @producer.produce_sync(topic: @topic_name, payload: "test")

              raise StandardError
            end
          end

          assert_equal(0, @client.messages.size)
          assert_empty(@client.messages_for("test"))
        end
      end
    end

    context "when running a nested transaction" do
      it "expect to work ok" do
        @producer.stub(:client, @client) do
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

    context "when we try to store offset without a transaction" do
      before do
        @topic = "it-#{SecureRandom.uuid}"
        @message = @buffered_message_stub.new(topic: @topic, partition: 0, offset: 10)
      end

      it "expect to raise an error" do
        @producer.stub(:client, @client) do
          assert_raises(WaterDrop::Errors::TransactionRequiredError) do
            @producer.transaction_mark_as_consumed(nil, @message)
          end
        end
      end
    end

    context "when trying to store offset with transaction" do
      before do
        @topic = "it-#{SecureRandom.uuid}"
        @consumer = @buffered_consumer_stub.new(consumer_group_metadata_pointer: nil)
        @message = @buffered_message_stub.new(topic: @topic, partition: 0, offset: 10)
      end

      it do
        @producer.stub(:client, @client) do
          @producer.transaction do
            @producer.transaction_mark_as_consumed(@consumer, @message)
          end
        end
      end
    end
  end
end
