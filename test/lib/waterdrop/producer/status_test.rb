# frozen_string_literal: true

require "test_helper"

describe_current do
  before do
    @status = described_class.new
  end

  it { assert_equal(true, @status.initial?) }

  context "when in the initial state" do
    before { @status.initial! }

    it { assert_equal(true, @status.initial?) }
    it { assert_equal(false, @status.configured?) }
    it { assert_equal(false, @status.active?) }
    it { assert_equal(false, @status.connected?) }
    it { assert_equal(false, @status.disconnecting?) }
    it { assert_equal(false, @status.disconnected?) }
    it { assert_equal(false, @status.closing?) }
    it { assert_equal(false, @status.closed?) }
    it { assert_equal("initial", @status.to_s) }
  end

  context "when in the configured state" do
    before { @status.configured! }

    it { assert_equal(false, @status.initial?) }
    it { assert_equal(true, @status.configured?) }
    it { assert_equal(true, @status.active?) }
    it { assert_equal(false, @status.connected?) }
    it { assert_equal(false, @status.disconnecting?) }
    it { assert_equal(false, @status.disconnected?) }
    it { assert_equal(false, @status.closing?) }
    it { assert_equal(false, @status.closed?) }
    it { assert_equal("configured", @status.to_s) }
  end

  context "when in the connected state" do
    before { @status.connected! }

    it { assert_equal(false, @status.initial?) }
    it { assert_equal(false, @status.configured?) }
    it { assert_equal(true, @status.active?) }
    it { assert_equal(true, @status.connected?) }
    it { assert_equal(false, @status.disconnecting?) }
    it { assert_equal(false, @status.disconnected?) }
    it { assert_equal(false, @status.closing?) }
    it { assert_equal(false, @status.closed?) }
    it { assert_equal("connected", @status.to_s) }
  end

  context "when in the disconnecting state" do
    before { @status.disconnecting! }

    it { assert_equal(false, @status.initial?) }
    it { assert_equal(false, @status.configured?) }
    it { assert_equal(true, @status.active?) }
    it { assert_equal(false, @status.connected?) }
    it { assert_equal(true, @status.disconnecting?) }
    it { assert_equal(false, @status.disconnected?) }
    it { assert_equal(false, @status.closing?) }
    it { assert_equal(false, @status.closed?) }
    it { assert_equal("disconnecting", @status.to_s) }
  end

  context "when in the disconnected state" do
    before { @status.disconnected! }

    it { assert_equal(false, @status.initial?) }
    it { assert_equal(false, @status.configured?) }
    it { assert_equal(true, @status.active?) }
    it { assert_equal(false, @status.connected?) }
    it { assert_equal(false, @status.disconnecting?) }
    it { assert_equal(true, @status.disconnected?) }
    it { assert_equal(false, @status.closing?) }
    it { assert_equal(false, @status.closed?) }
    it { assert_equal("disconnected", @status.to_s) }
  end

  context "when in the closing state" do
    before { @status.closing! }

    it { assert_equal(false, @status.initial?) }
    it { assert_equal(false, @status.configured?) }
    it { assert_equal(false, @status.active?) }
    it { assert_equal(false, @status.connected?) }
    it { assert_equal(false, @status.disconnecting?) }
    it { assert_equal(false, @status.disconnected?) }
    it { assert_equal(true, @status.closing?) }
    it { assert_equal(false, @status.closed?) }
    it { assert_equal("closing", @status.to_s) }
  end

  context "when in the closed state" do
    before { @status.closed! }

    it { assert_equal(false, @status.initial?) }
    it { assert_equal(false, @status.configured?) }
    it { assert_equal(false, @status.active?) }
    it { assert_equal(false, @status.connected?) }
    it { assert_equal(false, @status.disconnecting?) }
    it { assert_equal(false, @status.disconnected?) }
    it { assert_equal(false, @status.closing?) }
    it { assert_equal(true, @status.closed?) }
    it { assert_equal("closed", @status.to_s) }
  end
end
