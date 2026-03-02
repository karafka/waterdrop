# frozen_string_literal: true

describe_current do
  before do
    @status = described_class.new
  end

  it { assert_predicate(@status, :initial?) }

  context "when in the initial state" do
    before { @status.initial! }

    it { assert_predicate(@status, :initial?) }
    it { refute_predicate(@status, :configured?) }
    it { refute_predicate(@status, :active?) }
    it { refute_predicate(@status, :connected?) }
    it { refute_predicate(@status, :disconnecting?) }
    it { refute_predicate(@status, :disconnected?) }
    it { refute_predicate(@status, :closing?) }
    it { refute_predicate(@status, :closed?) }
    it { assert_equal("initial", @status.to_s) }
  end

  context "when in the configured state" do
    before { @status.configured! }

    it { refute_predicate(@status, :initial?) }
    it { assert_predicate(@status, :configured?) }
    it { assert_predicate(@status, :active?) }
    it { refute_predicate(@status, :connected?) }
    it { refute_predicate(@status, :disconnecting?) }
    it { refute_predicate(@status, :disconnected?) }
    it { refute_predicate(@status, :closing?) }
    it { refute_predicate(@status, :closed?) }
    it { assert_equal("configured", @status.to_s) }
  end

  context "when in the connected state" do
    before { @status.connected! }

    it { refute_predicate(@status, :initial?) }
    it { refute_predicate(@status, :configured?) }
    it { assert_predicate(@status, :active?) }
    it { assert_predicate(@status, :connected?) }
    it { refute_predicate(@status, :disconnecting?) }
    it { refute_predicate(@status, :disconnected?) }
    it { refute_predicate(@status, :closing?) }
    it { refute_predicate(@status, :closed?) }
    it { assert_equal("connected", @status.to_s) }
  end

  context "when in the disconnecting state" do
    before { @status.disconnecting! }

    it { refute_predicate(@status, :initial?) }
    it { refute_predicate(@status, :configured?) }
    it { assert_predicate(@status, :active?) }
    it { refute_predicate(@status, :connected?) }
    it { assert_predicate(@status, :disconnecting?) }
    it { refute_predicate(@status, :disconnected?) }
    it { refute_predicate(@status, :closing?) }
    it { refute_predicate(@status, :closed?) }
    it { assert_equal("disconnecting", @status.to_s) }
  end

  context "when in the disconnected state" do
    before { @status.disconnected! }

    it { refute_predicate(@status, :initial?) }
    it { refute_predicate(@status, :configured?) }
    it { assert_predicate(@status, :active?) }
    it { refute_predicate(@status, :connected?) }
    it { refute_predicate(@status, :disconnecting?) }
    it { assert_predicate(@status, :disconnected?) }
    it { refute_predicate(@status, :closing?) }
    it { refute_predicate(@status, :closed?) }
    it { assert_equal("disconnected", @status.to_s) }
  end

  context "when in the closing state" do
    before { @status.closing! }

    it { refute_predicate(@status, :initial?) }
    it { refute_predicate(@status, :configured?) }
    it { refute_predicate(@status, :active?) }
    it { refute_predicate(@status, :connected?) }
    it { refute_predicate(@status, :disconnecting?) }
    it { refute_predicate(@status, :disconnected?) }
    it { assert_predicate(@status, :closing?) }
    it { refute_predicate(@status, :closed?) }
    it { assert_equal("closing", @status.to_s) }
  end

  context "when in the closed state" do
    before { @status.closed! }

    it { refute_predicate(@status, :initial?) }
    it { refute_predicate(@status, :configured?) }
    it { refute_predicate(@status, :active?) }
    it { refute_predicate(@status, :connected?) }
    it { refute_predicate(@status, :disconnecting?) }
    it { refute_predicate(@status, :disconnected?) }
    it { refute_predicate(@status, :closing?) }
    it { assert_predicate(@status, :closed?) }
    it { assert_equal("closed", @status.to_s) }
  end
end
