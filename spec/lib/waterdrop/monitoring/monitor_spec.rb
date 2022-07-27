# frozen_string_literal: true

RSpec.describe_current do
  subject(:monitor) do
    described_class.new(
      notifications_bus,
      namespace
    )
  end

  let(:notifications_bus) { WaterDrop::Instrumentation::Notifications.new }

  before do
    notifications_bus.register_event('test')
    notifications_bus.register_event('test.namespace')
  end

  context 'when we do not use any namespace' do
    let(:namespace) { nil }
    let(:collected_data) { [] }

    before do
      collected = collected_data

      notifications_bus.subscribe('test') do |event|
        collected << event
      end

      monitor.instrument('test') { 1 }
    end

    it { expect(collected_data.size).to eq(1) }
    it { expect(collected_data.first.id).to eq('test') }
  end

  context 'when we do use a namespace' do
    let(:namespace) { 'namespace' }
    let(:collected_data) { [] }

    before do
      collected = collected_data

      notifications_bus.subscribe('test.namespace') do |event|
        collected << event
      end

      monitor.instrument('test') { 1 }
    end

    it { expect(collected_data.size).to eq(1) }
    it { expect(collected_data.first.id).to eq('test.namespace') }
  end
end
