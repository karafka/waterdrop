# frozen_string_literal: true

RSpec.describe_current do
  subject(:callback) { described_class.new(producer_id, monitor) }

  let(:producer_id) { SecureRandom.uuid }
  let(:monitor) { ::WaterDrop::Instrumentation::Monitor.new }
  let(:delivery_report) { OpenStruct.new(offset: rand(100), partition: rand(100)) }

  describe '#call' do
    let(:changed) { [] }
    let(:event) { changed.first }

    before do
      monitor.subscribe('message.acknowledged') do |event|
        changed << event
      end

      callback.call(delivery_report)
    end

    it { expect(event.id).to eq('message.acknowledged') }
    it { expect(event.payload[:producer_id]).to eq(producer_id) }
    it { expect(event.payload[:offset]).to eq(delivery_report.offset) }
    it { expect(event.payload[:partition]).to eq(delivery_report.partition) }
  end
end
