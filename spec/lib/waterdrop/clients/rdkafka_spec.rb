# frozen_string_literal: true

RSpec.describe_current do
  subject(:client) { described_class.new(producer) }

  let(:producer) do
    WaterDrop::Producer.new do |config|
      config.deliver = false
      config.kafka = { 'bootstrap.servers': BOOTSTRAP_SERVERS }
    end
  end

  after do
    client.close
    producer.close
  end

  it { expect(client).to be_a(Rdkafka::Producer) }
end
