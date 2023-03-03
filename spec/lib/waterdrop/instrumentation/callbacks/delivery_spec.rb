# frozen_string_literal: true

RSpec.describe_current do
  subject(:callback) { described_class.new(producer_id, monitor) }

  let(:producer) { build(:producer) }
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
    it { expect(event[:producer_id]).to eq(producer_id) }
    it { expect(event[:offset]).to eq(delivery_report.offset) }
    it { expect(event[:partition]).to eq(delivery_report.partition) }
  end

  describe '#when we do an end-to-end delivery report check' do
    context 'when there is a message that was successfully delivered' do
      let(:changed) { [] }
      let(:event) { changed.first }

      before do
        producer.monitor.subscribe('message.acknowledged') do |event|
          changed << event
        end

        producer.produce_sync(build(:valid_message))
      end

      it { expect(event.payload[:partition]).to eq(0) }
      it { expect(event.payload[:offset]).to eq(0) }
    end

    context 'when there is a message that was not successfully delivered async' do
      let(:changed) { [] }
      let(:event) { changed.first }

      before do
        producer.monitor.subscribe('error.occurred') do |event|
          changed << event
        end

        # We force it to bypass the validations, so we trigger an error on delivery
        # otherwise we would be stopped by WaterDrop itself
        producer.send(:client).produce(topic: '$%^&*', payload: '1')

        sleep(0.01) until changed.size.positive?
      end

      it { expect(event.payload[:error]).to be_a(Rdkafka::RdkafkaError) }
      it { expect(event.payload[:partition]).to eq(-1) }
      it { expect(event.payload[:offset]).to eq(-1001) }
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
      end

      it { expect(errors.first).to be_a(WaterDrop::Errors::ProduceError) }
      it { expect(errors.first.cause).to be_a(Rdkafka::RdkafkaError) }
      it { expect(event[:error]).to be_a(WaterDrop::Errors::ProduceError) }
      it { expect(event[:error].cause).to be_a(Rdkafka::RdkafkaError) }
    end
  end
end
