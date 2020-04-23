# frozen_string_literal: true

RSpec.describe WaterDrop::Producer do
  subject(:producer) { described_class.new }

  describe '#initialize' do
    context 'when we initialize without a setup' do
      it { expect { producer }.not_to raise_error }
      it { expect(producer.status.active?).to eq(false) }
    end

    context 'when initializing with setup' do
      subject(:producer) do
        described_class.new do |config|
          config.deliver = false
          config.kafka = { 'bootstrap.servers' => 'localhost:9092' }
        end
      end

      it { expect { producer }.not_to raise_error }
      it { expect(producer.status.active?).to eq(true) }
    end
  end

  describe '#setup' do
    context 'when producer has already been configured' do
      subject(:producer) { build(:producer) }

      let(:expected_error) { WaterDrop::Errors::ProducerAlreadyConfiguredError }

      it { expect { producer.setup {} }.to raise_error(expected_error) }
    end

    context 'when producer was not yet configured' do
      let(:setup) do
        lambda { |config|
          config.deliver = false
          config.kafka = { 'bootstrap.servers' => 'localhost:9092' }
        }
      end

      it { expect { producer.setup(&setup) }.not_to raise_error }
    end
  end

  describe '#close' do
    subject(:producer) { build(:producer) }

    context 'when producer already closed' do
      before { producer.close }

      it { expect { producer.close }.not_to raise_error }
      it { expect(producer.tap(&:close).status.closed?).to eq(true) }
    end

    context 'when producer was not yet closed' do
      it { expect { producer.close }.not_to raise_error }
      it { expect(producer.tap(&:close).status.closed?).to eq(true) }
    end

    context 'when there were messages in the buffer' do
      before { producer.buffer(build(:valid_message)) }

      it { expect { producer.close }.to change { producer.messages.size }.from(1).to(0) }
    end
  end

  describe '#ensure_active!' do
    subject(:producer) { create(:producer) }

    context 'when status is invalid' do
      let(:expected_error) { WaterDrop::Errors::StatusInvalidError }

      before do
        allow(producer.status).to receive(:active?).and_return(false)
        allow(producer.status).to receive(:initial?).and_return(false)
        allow(producer.status).to receive(:closing?).and_return(false)
        allow(producer.status).to receive(:closed?).and_return(false)
      end

      it { expect { producer.send(:ensure_active!) }.to raise_error(expected_error) }
    end
  end
end
