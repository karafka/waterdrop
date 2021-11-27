# frozen_string_literal: true

RSpec.describe_current do
  subject(:callback) { described_class.new(producer_id, monitor) }

  let(:producer_id) { SecureRandom.uuid }
  let(:monitor) { ::WaterDrop::Instrumentation::Monitor.new }
  let(:delivery_report) { OpenStruct.new(offset: rand(100), partition: rand(100)) }

  describe '#call' do
    pending
    # it 'expect to emit proper event with details' do
    # end
  end
end
