# frozen_string_literal: true

RSpec.describe_current do
  subject(:status) { described_class.new }

  it { expect(status.initial?).to be(true) }

  context "when in the initial state" do
    before { status.initial! }

    it { expect(status.initial?).to be(true) }
    it { expect(status.configured?).to be(false) }
    it { expect(status.active?).to be(false) }
    it { expect(status.connected?).to be(false) }
    it { expect(status.disconnecting?).to be(false) }
    it { expect(status.disconnected?).to be(false) }
    it { expect(status.closing?).to be(false) }
    it { expect(status.closed?).to be(false) }
    it { expect(status.to_s).to eq("initial") }
  end

  context "when in the configured state" do
    before { status.configured! }

    it { expect(status.initial?).to be(false) }
    it { expect(status.configured?).to be(true) }
    it { expect(status.active?).to be(true) }
    it { expect(status.connected?).to be(false) }
    it { expect(status.disconnecting?).to be(false) }
    it { expect(status.disconnected?).to be(false) }
    it { expect(status.closing?).to be(false) }
    it { expect(status.closed?).to be(false) }
    it { expect(status.to_s).to eq("configured") }
  end

  context "when in the connected state" do
    before { status.connected! }

    it { expect(status.initial?).to be(false) }
    it { expect(status.configured?).to be(false) }
    it { expect(status.active?).to be(true) }
    it { expect(status.connected?).to be(true) }
    it { expect(status.disconnecting?).to be(false) }
    it { expect(status.disconnected?).to be(false) }
    it { expect(status.closing?).to be(false) }
    it { expect(status.closed?).to be(false) }
    it { expect(status.to_s).to eq("connected") }
  end

  context "when in the disconnecting state" do
    before { status.disconnecting! }

    it { expect(status.initial?).to be(false) }
    it { expect(status.configured?).to be(false) }
    it { expect(status.active?).to be(true) }
    it { expect(status.connected?).to be(false) }
    it { expect(status.disconnecting?).to be(true) }
    it { expect(status.disconnected?).to be(false) }
    it { expect(status.closing?).to be(false) }
    it { expect(status.closed?).to be(false) }
    it { expect(status.to_s).to eq("disconnecting") }
  end

  context "when in the disconnected state" do
    before { status.disconnected! }

    it { expect(status.initial?).to be(false) }
    it { expect(status.configured?).to be(false) }
    it { expect(status.active?).to be(true) }
    it { expect(status.connected?).to be(false) }
    it { expect(status.disconnecting?).to be(false) }
    it { expect(status.disconnected?).to be(true) }
    it { expect(status.closing?).to be(false) }
    it { expect(status.closed?).to be(false) }
    it { expect(status.to_s).to eq("disconnected") }
  end

  context "when in the closing state" do
    before { status.closing! }

    it { expect(status.initial?).to be(false) }
    it { expect(status.configured?).to be(false) }
    it { expect(status.active?).to be(false) }
    it { expect(status.connected?).to be(false) }
    it { expect(status.disconnecting?).to be(false) }
    it { expect(status.disconnected?).to be(false) }
    it { expect(status.closing?).to be(true) }
    it { expect(status.closed?).to be(false) }
    it { expect(status.to_s).to eq("closing") }
  end

  context "when in the closed state" do
    before { status.closed! }

    it { expect(status.initial?).to be(false) }
    it { expect(status.configured?).to be(false) }
    it { expect(status.active?).to be(false) }
    it { expect(status.connected?).to be(false) }
    it { expect(status.disconnecting?).to be(false) }
    it { expect(status.disconnected?).to be(false) }
    it { expect(status.closing?).to be(false) }
    it { expect(status.closed?).to be(true) }
    it { expect(status.to_s).to eq("closed") }
  end
end
