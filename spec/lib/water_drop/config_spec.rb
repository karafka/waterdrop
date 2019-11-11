# frozen_string_literal: true

RSpec.describe WaterDrop::Config do
  subject(:config_class) { described_class }

  describe '#setup' do
    it { expect { |block| config_class.setup(&block) }.to yield_with_args }
  end

  describe '#validate!' do
    context 'when configuration has errors' do
      let(:error_class) { ::WaterDrop::Errors::InvalidConfiguration }
      let(:error_message) { { client_id: ['must be filled'] }.to_s }
      let(:setup) do
        WaterDrop.setup do |config|
          config.client_id = nil
        end
      end

      after do
        WaterDrop.setup do |config|
          config.client_id = rand(100).to_s
        end
      end

      it 'raise InvalidConfiguration exception' do
        expect { setup }.to raise_error do |error|
          expect(error).to be_a(error_class)
          expect(error.message).to eq(error_message)
        end
      end
    end

    context 'when configuration is valid' do
      it 'not raise InvalidConfiguration exception' do
        expect { config_class.send(:validate!, WaterDrop.config.to_h) }
          .not_to raise_error
      end
    end
  end
end
