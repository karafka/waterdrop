# frozen_string_literal: true

RSpec.describe_current do
  subject(:config) { described_class.new }

  describe '#setup' do
    context 'when configuration has errors' do
      let(:error_class) { ::WaterDrop::Errors::ConfigurationInvalidError }
      let(:setup) { described_class.new.setup { |config| config.kafka = { 'a' => true } } }

      it 'raise ConfigurationInvalidError exception' do
        expect { setup }.to raise_error do |error|
          expect(error).to be_a(error_class)
        end
      end
    end

    context 'when configuration is valid' do
      let(:kafka_config) do
        { 'bootstrap.servers': 'localhost:9092', rand.to_s.to_sym => rand }
      end

      it 'not raise ConfigurationInvalidError exception' do
        expect { config.setup { |config| config.kafka = kafka_config } }
          .not_to raise_error
      end
    end

    context 'when we try to create and use transactional producer without idempotence' do
      subject(:producer) { build(:transactional_producer, idempotent: false) }

      it 'expect not to allow it' do
        expect do
          producer.produce_sync(topic: 'test', payload: 'test')
        end.to raise_error(Rdkafka::Config::ClientCreationError)
      end
    end
  end
end
