# frozen_string_literal: true

describe_current do
  before do
    @producer_inst = WaterDrop::Producer.new
    @deliver = true
  end

  after do
    @client.close
    @producer_inst.close
  end

  describe "client building" do
    before do
      should_deliver = @deliver

      producer_config = WaterDrop::Config.new

      producer_config.setup do |config|
        config.deliver = should_deliver
        config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
      end

      @config = producer_config.config

      @producer_inst.stub(:config, @config) do
        @client = described_class.new.call(@producer_inst, @config)
      end
    end

    it { assert_kind_of(Rdkafka::Producer, @client) }
    it { assert_kind_of(WaterDrop::Instrumentation::Callbacks::Delivery, @client.delivery_callback) }

    context "when the delivery is off" do
      before do
        @prev_client = @client
        @deliver = false

        producer_config = WaterDrop::Config.new

        producer_config.setup do |config|
          config.deliver = false
          config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
        end

        @config = producer_config.config

        @producer_inst.stub(:config, @config) do
          @client = described_class.new.call(@producer_inst, @config)
        end
      end

      after { @prev_client&.close }

      it { assert_kind_of(WaterDrop::Clients::Dummy, @client) }
    end

    context "when the delivery_callback is executed" do
      before do
        @prev_client = @client
        @delivery_report = Rdkafka::Producer::DeliveryReport.new(rand, rand)
        @callback_event = nil

        @config.monitor.subscribe("message.acknowledged") do |event|
          @callback_event = event
        end

        @producer_inst.stub(:config, @config) do
          @client = described_class.new.call(@producer_inst, @config)
        end

        @client.delivery_callback.call(@delivery_report)
      end

      after { @prev_client&.close }

      it { assert_equal(@delivery_report.offset, @callback_event[:offset]) }
      it { assert_equal(@delivery_report.partition, @callback_event[:partition]) }
      it { assert_nil(@callback_event[:producer_id]) }
    end
  end
end
