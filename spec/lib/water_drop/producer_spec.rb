# frozen_string_literal: true

RSpec.describe WaterDrop::Producer do
  describe '#initialize' do
    pending
  end

  describe '#setup' do
    pending
  end

  describe '#close' do
    pending
  end

  describe '#ensure_active!' do
    subject(:producer) {  create(:producer) }

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
