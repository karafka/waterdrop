# frozen_string_literal: true

RSpec.describe WaterDrop::Producer::Builder do
  subject(:client) { described_class.new.call(producer, config) }

  let(:producer) { WaterDrop::Producer.new }
  let(:deliver) { true }
  let(:config) do
    should_deliver = deliver

    producer_config = WaterDrop::Config.new

    producer_config.setup do |config|
      config.deliver = should_deliver
      config.kafka = { 'bootstrap.servers' => 'localhost:9092' }
    end

    producer_config.config
  end

  it { expect(client).to be_a(Rdkafka::Producer) }
  it { expect(client.delivery_callback).to be_a(Proc) }

  context 'when the delivery is off' do
    let(:deliver) { false }

    it { expect(client).to be_a(WaterDrop::Producer::DummyClient) }
  end

  context 'when the delivery_callback is executed' do
    let(:delivery_report) { ::Rdkafka::Producer::DeliveryReport.new(rand, rand) }
    let(:callback_event) do
      callback_event = nil

      config.monitor.subscribe('message.acknowledged') do |event|
        callback_event = event
      end

      client.delivery_callback.call(delivery_report)
      callback_event
    end

    it { expect(callback_event[:offset]).to eq(delivery_report.offset) }
    it { expect(callback_event[:partition]).to eq(delivery_report.partition) }
    it { expect(callback_event[:producer]).to eq(producer) }
  end
end
