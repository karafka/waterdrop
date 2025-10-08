# frozen_string_literal: true

RSpec.describe_current do
  subject(:callback) { described_class.new(producer_id, transactional, monitor) }

  let(:delivery_report_stub) do
    Struct.new(:offset, :partition, :topic_name, :error, :label, keyword_init: true)
  end
  let(:producer) { build(:producer) }
  let(:producer_id) { SecureRandom.uuid }
  let(:transactional) { producer.transactional? }
  let(:monitor) { WaterDrop::Instrumentation::Monitor.new }
  let(:delivery_report) do
    delivery_report_stub.new(
      offset: rand(100),
      partition: rand(100),
      topic_name: rand(100).to_s,
      error: 0,
      label: nil
    )
  end

  after { producer.close }

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
    it { expect(event[:producer_id]).to eq(producer_id) }
    it { expect(event[:offset]).to eq(delivery_report.offset) }
    it { expect(event[:partition]).to eq(delivery_report.partition) }
    it { expect(event[:topic]).to eq(delivery_report.topic_name) }

    context 'when delivery handler code contains an error' do
      let(:tracked_errors) { [] }

      before do
        monitor.subscribe('message.acknowledged') do
          raise
        end

        local_errors = tracked_errors

        monitor.subscribe('error.occurred') do |event|
          local_errors << event
        end
      end

      it 'expect to contain in, notify and continue as we do not want to crash rdkafka' do
        expect { callback.call(delivery_report) }.not_to raise_error
        expect(tracked_errors.size).to eq(1)
        expect(tracked_errors.first[:type]).to eq('callbacks.delivery.error')
      end
    end
  end

  describe '#when we do an end-to-end delivery report check' do
    context 'when there is a message that was successfully delivered' do
      let(:changed) { [] }
      let(:event) { changed.first }
      let(:message) { build(:valid_message) }

      before do
        producer.monitor.subscribe('message.acknowledged') do |event|
          changed << event
        end

        producer.produce_sync(message)

        sleep(0.01) until changed.size.positive?
      end

      it { expect(event.payload[:partition]).to eq(0) }
      it { expect(event.payload[:offset]).to eq(0) }
      it { expect(event[:topic]).to eq(message[:topic]) }
    end

    context 'when there is a message that was not successfully delivered async' do
      let(:changed) { [] }
      let(:event) { changed.last }

      before do
        producer.monitor.subscribe('error.occurred') do |event|
          changed << event
        end

        100.times do
          # We force it to bypass the validations, so we trigger an error on delivery
          # otherwise we would be stopped by WaterDrop itself
          producer.send(:client).produce(topic: '$%^&*', payload: '1')
        rescue Rdkafka::RdkafkaError
          nil
        end

        sleep(0.01) until changed.size.positive?
      end

      it { expect(event.payload[:error]).to be_a(Rdkafka::RdkafkaError) }
      it { expect(event.payload[:partition]).to eq(-1) }
      it { expect(event.payload[:offset]).to eq(-1001) }
      it { expect(event.payload[:topic]).to eq('$%^&*') }
    end

    context 'when there is a message that was not successfully delivered sync' do
      let(:changed) { [] }
      let(:event) { changed.first }

      before do
        producer.monitor.subscribe('error.occurred') do |event|
          changed << event
        end

        # Intercept the error so it won't bubble up as we want to check the notifications pipeline
        begin
          producer.send(:client).produce(topic: '$%^&*', payload: '1').wait
        rescue Rdkafka::RdkafkaError
          nil
        end

        sleep(0.01) until changed.size.positive?
      end

      it { expect(event.payload[:error]).to be_a(Rdkafka::RdkafkaError) }
      it { expect(event.payload[:partition]).to eq(-1) }
      it { expect(event.payload[:offset]).to eq(-1001) }
    end

    context 'when there is an inline thrown erorrs' do
      let(:changed) { [] }
      let(:errors) { [] }
      let(:event) { changed.first }
      let(:producer) { build(:limited_producer) }

      before do
        producer.monitor.subscribe('error.occurred') do |event|
          changed << event
        end

        # Intercept the error so it won't bubble up as we want to check the notifications pipeline
        begin
          msg = build(:valid_message)
          100.times { producer.produce_async(msg) }
        rescue WaterDrop::Errors::ProduceError => e
          errors << e
        end

        sleep(0.01) until changed.size.positive?
      end

      it { expect(errors.first).to be_a(WaterDrop::Errors::ProduceError) }
      it { expect(errors.first.cause).to be_a(Rdkafka::RdkafkaError) }
      it { expect(event[:error]).to be_a(WaterDrop::Errors::ProduceError) }
      it { expect(event[:error].cause).to be_a(Rdkafka::RdkafkaError) }
    end

    context 'when there is a producer with non-transactional purge' do
      let(:producer) { build(:slow_producer) }
      let(:errors) { [] }
      let(:purges) { [] }

      before do
        producer.monitor.subscribe('error.occurred') do |event|
          errors << event[:error]
        end

        producer.monitor.subscribe('message.purged') do |event|
          purges << event[:error]
        end

        producer.produce_async(build(:valid_message))
        producer.purge

        sleep(0.01) until errors.size.positive?
      end

      it 'expect to have it in the errors' do
        expect(errors.first).to be_a(Rdkafka::RdkafkaError)
        expect(errors.first.code).to eq(:purge_queue)
      end

      it 'expect not to publish purge notification' do
        expect(purges).to be_empty
      end
    end
  end
end
