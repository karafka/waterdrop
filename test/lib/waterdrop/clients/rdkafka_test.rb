# frozen_string_literal: true

describe_current do
  before do
    @producer = WaterDrop::Producer.new do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end
    @client = described_class.new(@producer)
  end

  after do
    @client.close
    @producer.close
  end

  it { assert_kind_of(Rdkafka::Producer, @client) }
end
