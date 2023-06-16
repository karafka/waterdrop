# frozen_string_literal: true

RSpec.describe_current do
  subject(:client) { described_class.new(producer) }

  let(:producer) do
    WaterDrop::Producer.new do |config|
      config.deliver = false
      config.kafka = { 'bootstrap.servers': 'localhost:9092' }
    end
  end

  after { producer.close }

  it { expect(client).to be_a(Rdkafka::Producer) }
end
