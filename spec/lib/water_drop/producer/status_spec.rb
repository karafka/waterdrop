# frozen_string_literal: true

RSpec.describe WaterDrop::Producer::Status do
  subject(:status) { described_class.new }

  it { expect(status.initial?).to eq(true) }

  context 'when in the initial state' do
    before { status.initial! }

    it { expect(status.initial?).to eq(true) }
    it { expect(status.active?).to eq(false) }
    it { expect(status.closing?).to eq(false) }
    it { expect(status.closed?).to eq(false) }
    it { expect(status.to_s).to eq('initial') }
  end

  context 'when in the active state' do
    before { status.active! }

    it { expect(status.initial?).to eq(false) }
    it { expect(status.active?).to eq(true) }
    it { expect(status.closing?).to eq(false) }
    it { expect(status.closed?).to eq(false) }
    it { expect(status.to_s).to eq('active') }
  end

  context 'when in the closing state' do
    before { status.closing! }

    it { expect(status.initial?).to eq(false) }
    it { expect(status.active?).to eq(false) }
    it { expect(status.closing?).to eq(true) }
    it { expect(status.closed?).to eq(false) }
    it { expect(status.to_s).to eq('closing') }
  end

  context 'when in the closed state' do
    before { status.closed! }

    it { expect(status.initial?).to eq(false) }
    it { expect(status.active?).to eq(false) }
    it { expect(status.closing?).to eq(false) }
    it { expect(status.closed?).to eq(true) }
    it { expect(status.to_s).to eq('closed') }
  end
end
