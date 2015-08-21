require 'spec_helper'

RSpec.describe WaterDrop::Config do
  subject { described_class.new }

  described_class::OPTIONS.each do |attribute|
    describe "#{attribute}=" do
      let(:value) { rand }
      before { subject.public_send(:"#{attribute}=", value) }

      it 'assigns a given value' do
        expect(subject.public_send(attribute)).to eq value
      end
    end
  end

  describe '#send_messages?' do
    context 'when we dont want to send events' do
      before { subject.send_messages = false }

      it { expect(subject.send_messages?).to eq false }
    end

    context 'whe we want to send events' do
      before { subject.send_messages = true }

      it { expect(subject.send_messages?).to eq true }
    end
  end

  describe '.setup' do
    subject { described_class }
    let(:instance) { described_class.new }
    let(:block) { -> {} }

    before do
      instance

      expect(subject)
        .to receive(:new)
        .and_return(instance)

      expect(block)
        .to receive(:call)
        .with(instance)

      expect(instance)
        .to receive(:freeze)
    end

    it { subject.setup(&block) }
  end
end
