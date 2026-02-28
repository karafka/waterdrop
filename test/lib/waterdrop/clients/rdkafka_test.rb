# frozen_string_literal: true

class WaterDropClientsRdkafkaTest < WaterDropTest::Base
  def setup
    @producer = WaterDrop::Producer.new do |config|
      config.deliver = false
      config.kafka = { "bootstrap.servers": BOOTSTRAP_SERVERS }
    end
    @client = WaterDrop::Clients::Rdkafka.new(@producer)
  end

  def teardown
    @client.close
    @producer.close
    super
  end

  def test_client_is_an_rdkafka_producer
    assert_kind_of Rdkafka::Producer, @client
  end
end
