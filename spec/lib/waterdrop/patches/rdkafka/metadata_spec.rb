# frozen_string_literal: true

RSpec.describe_current do
  subject(:partition_count) { producer.client.partition_count('example_topic') }

  let(:producer) do
    WaterDrop::Producer.new do |config|
      config.deliver = true
      config.kafka = { 'bootstrap.servers': '127.0.0.1:9092' }
    end
  end

  after { producer.close }

  describe 'metadata initialization recovery' do
    context 'when all good' do
      it { expect(partition_count).to eq(1) }
    end

    context 'when we fail for the first time with handled error' do
      before do
        raised = false

        allow(Rdkafka::Bindings).to receive(:rd_kafka_metadata).and_wrap_original do |m, *args|
          if raised
            m.call(*args)
          else
            raised = true
            -185
          end
        end
      end

      it { expect(partition_count).to eq(1) }
    end
  end
end
