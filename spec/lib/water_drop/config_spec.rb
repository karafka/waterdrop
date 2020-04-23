# frozen_string_literal: true

RSpec.describe WaterDrop::Config do
  subject(:config) { described_class.new }

  describe '#setup' do
    context 'when configuration has errors' do
      let(:error_class) { ::WaterDrop::Errors::ConfigurationInvalidError }
      let(:error_message) { { kafka: { 'bootstrap.servers': ['is missing'] } }.to_s }
      let(:setup) { described_class.new.setup {} }

      it 'raise ConfigurationInvalidError exception' do
        expect { setup }.to raise_error do |error|
          expect(error).to be_a(error_class)
          expect(error.message).to eq(error_message)
        end
      end
    end

    context 'when configuration is valid' do
      let(:kafka_config) do
        { 'bootstrap.servers' => 'localhost:9092', rand => rand }
      end

      it 'not raise ConfigurationInvalidError exception' do
        expect { config.setup { |config| config.kafka = kafka_config } }
          .not_to raise_error
      end
    end
  end
end
