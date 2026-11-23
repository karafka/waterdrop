# frozen_string_literal: true

describe_current do
  before do
    @contract = described_class.new
    @config = { kafka: { "bootstrap.servers": "localhost:9092" } }
  end

  describe "when bootstrap servers are a non-empty string" do
    it { assert_predicate @contract.call(@config), :success? }
  end

  describe "when bootstrap servers are missing" do
    before { @config[:kafka] = { "client.id": "id" } }

    it { refute_predicate @contract.call(@config), :success? }
    it { assert_equal({ "kafka.bootstrap.servers": "must be present" }, @contract.call(@config).errors) }
  end

  describe "when bootstrap servers are nil" do
    before { @config[:kafka][:"bootstrap.servers"] = nil }

    it { refute_predicate @contract.call(@config), :success? }

    it "expect to name the setting" do
      assert_equal(
        { "kafka.bootstrap.servers": "must be a non-empty string" },
        @contract.call(@config).errors
      )
    end
  end

  describe "when bootstrap servers are an empty string" do
    before { @config[:kafka][:"bootstrap.servers"] = "" }

    it { refute_predicate @contract.call(@config), :success? }
  end

  describe "when bootstrap servers are not a string" do
    before { @config[:kafka][:"bootstrap.servers"] = ["localhost:9092"] }

    it { refute_predicate @contract.call(@config), :success? }
  end
end
