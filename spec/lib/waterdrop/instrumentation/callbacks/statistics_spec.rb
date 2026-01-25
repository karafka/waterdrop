# frozen_string_literal: true

RSpec.describe_current do
  subject(:callback) { described_class.new(producer_id, client_name, monitor) }

  let(:producer_id) { SecureRandom.uuid }
  let(:client_name) { SecureRandom.uuid }
  let(:monitor) { WaterDrop::Instrumentation::Monitor.new }

  describe "#call" do
    let(:changed) { [] }
    let(:statistics) { {} }

    before do
      monitor.subscribe("statistics.emitted") do |event|
        changed << event[:statistics]
      end

      callback.call(statistics)
    end

    context "when emitted statistics refer different producer" do
      it "expect not to emit them" do
        expect(changed).to be_empty
      end
    end

    context "when emitted statistics handler code contains an error" do
      let(:statistics) { { "name" => client_name } }
      let(:tracked_errors) { [] }

      before do
        monitor.subscribe("statistics.emitted") do
          raise
        end

        local_errors = tracked_errors

        monitor.subscribe("error.occurred") do |event|
          local_errors << event
        end
      end

      it "expect to contain in, notify and continue as we do not want to crash rdkafka" do
        expect { callback.call(statistics) }.not_to raise_error
        expect(tracked_errors.size).to eq(1)
        expect(tracked_errors.first[:type]).to eq("callbacks.statistics.error")
      end
    end

    context "when emitted statistics refer to expected producer" do
      let(:statistics) { { "name" => client_name } }

      it "expects to emit them" do
        expect(changed).to eq([statistics])
      end
    end

    context "when we emit more statistics" do
      before do
        5.times do |count|
          callback.call("msg_count" => count, "name" => client_name)
        end
      end

      it { expect(changed.size).to eq(5) }

      it "expect to decorate them" do
        # First is also decorated but wit no change
        expect(changed.first["msg_count_d"]).to eq(0)
        expect(changed.last["msg_count_d"]).to eq(1)
      end
    end
  end

  describe "emitted event data format" do
    let(:events) { [] }
    let(:event) { events.first }
    let(:statistics) { { "name" => client_name, "val" => 1, "str" => 1 } }

    before do
      monitor.subscribe("statistics.emitted") do |event|
        events << event
      end

      callback.call(statistics)
    end

    it { expect(event.id).to eq("statistics.emitted") }
    it { expect(event[:producer_id]).to eq(producer_id) }
    it { expect(event[:statistics]).to eq(statistics) }
    it { expect(event[:statistics]["val_d"]).to eq(0) }
  end

  describe "late subscription support" do
    let(:events) { [] }
    let(:statistics) { { "name" => client_name, "msg_count" => 100 } }

    context "when no one is listening initially" do
      it "expect not to emit statistics" do
        callback.call(statistics)
        expect(events).to be_empty
      end
    end

    context "when subscriber is added after initialization" do
      it "expect to emit statistics for subsequent calls" do
        # First call with no listeners
        callback.call(statistics)
        expect(events).to be_empty

        # Subscribe late
        monitor.subscribe("statistics.emitted") do |event|
          events << event
        end

        # Second call should now emit
        callback.call(statistics)
        expect(events).not_to be_empty
        expect(events.size).to eq(1)
      end
    end
  end
end
