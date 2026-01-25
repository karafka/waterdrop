# frozen_string_literal: true

RSpec.describe_current do
  subject(:callback) { described_class.new(producer_id, client_name, monitor) }

  let(:producer_id) { SecureRandom.uuid }
  let(:client_name) { SecureRandom.uuid }
  let(:monitor) { WaterDrop::Instrumentation::Monitor.new }
  let(:error) { Rdkafka::RdkafkaError.new(1, []) }

  describe "#call" do
    let(:changed) { [] }

    before do
      monitor.subscribe("error.occurred") do |event|
        changed << event[:error]
      end

      callback.call(client_name, error)
    end

    context "when occurred error refer different producer" do
      subject(:callback) { described_class.new(producer_id, "other", monitor) }

      it "expect not to emit them" do
        expect(changed).to be_empty
      end
    end

    context "when occurred error refer to expected producer" do
      it "expects to emit them" do
        expect(changed).to eq([error])
      end
    end
  end

  describe "occurred event data format" do
    let(:changed) { [] }
    let(:event) { changed.first }

    before do
      monitor.subscribe("error.occurred") do |stat|
        changed << stat
      end

      callback.call(client_name, error)
    end

    it { expect(event.id).to eq("error.occurred") }
    it { expect(event[:producer_id]).to eq(producer_id) }
    it { expect(event[:error]).to eq(error) }
    it { expect(event[:type]).to eq("librdkafka.error") }
  end

  context "when librdkafka error handling handler contains error" do
    let(:tracked_errors) { [] }

    before do
      monitor.subscribe("error.occurred") do |event|
        next unless event[:type] == "librdkafka.error"

        raise
      end

      local_errors = tracked_errors

      monitor.subscribe("error.occurred") do |event|
        local_errors << event
      end
    end

    it "expect to contain in, notify and continue as we do not want to crash rdkafka" do
      expect { callback.call(client_name, error) }.not_to raise_error
      expect(tracked_errors.size).to eq(1)
      expect(tracked_errors.first[:type]).to eq("callbacks.error.error")
    end
  end
end
