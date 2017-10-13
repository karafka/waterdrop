# frozen_string_literal: true

RSpec.describe WaterDrop::ProducerProxy do
  subject(:producer_proxy) { described_class.new }

  describe 'ruby-kafka parameters integration' do
    let(:parameters) { Kafka::Client.instance_method(:initialize).parameters.map(&:last).sort }

    it 'expect to match ruby-kafka client requirements' do
      # Client id and logger are taken from the main config scope
      expect(WaterDrop.config.kafka.to_h.keys.sort).to eq(parameters - %i[client_id logger])
    end
  end

  describe '#send_message' do
    let(:producer) { double }
    let(:message_options) { {} }
    let(:message) do
      instance_double(WaterDrop::Message,
                      message: rand,
                      topic: rand,
                      options: message_options)
    end

    context 'when sending was successful (no errors)' do
      before do
        allow(producer_proxy)
          .to receive(:producer)
          .and_return(producer)
          .exactly(2).times

        allow(producer_proxy)
          .to receive(:touch)
      end

      it 'expect to touch and forward to producer' do
        expect(producer)
          .to receive(:produce)
          .with(message.message, topic: message.topic)

        expect(producer).to receive(:deliver_messages)

        producer_proxy.send_message(message)
      end

      context 'with optional producer arguments' do
        let(:message_options) { { partition: rand, partition_key: rand } }

        it 'expect to forward to producer' do
          expect(producer)
            .to receive(:produce)
            .with(message.message, { topic: message.topic }.merge(message_options))

          expect(producer).to receive(:deliver_messages)

          producer_proxy.send_message(message)
        end
      end
    end

    context 'when something went wrong' do
      let(:error) { StandardError }

      before do
        expect(producer_proxy)
          .to receive(:producer)
          .and_return(producer)
          .exactly(2).times

        expect(producer_proxy)
          .to receive(:touch)
          .exactly(2).times

        expect(producer)
          .to receive(:produce)
          .with(message.message, topic: message.topic)
          .and_raise(error)
          .exactly(2).times
      end

      it 'expect to reload producer retry once and if fails again reraise error' do
        expect(producer_proxy)
          .to receive(:reload!)
          .exactly(2).times

        expect { producer_proxy.send_message(message) }.to raise_error(error)
      end
    end
  end

  describe '#touch' do
    let(:now) { rand }

    before do
      producer_proxy

      expect(Time)
        .to receive(:now)
        .and_return(now)
    end

    it 'expect to update time to time now' do
      producer_proxy.send :touch
      expect(producer_proxy.instance_variable_get(:@last_usage)).to eq now
    end
  end

  describe '#producer' do
    let(:kafka) { double }
    let(:kafka_init_parameters) do
      {
        client_id: ::WaterDrop.config.client_id,
        logger: ::WaterDrop.config.logger
      }.merge(::WaterDrop.config.kafka.to_h)
    end
    let(:producer_init_parameters) do
      {
        compression_codec: ::WaterDrop.config.producer.compression_codec,
        compression_threshold: ::WaterDrop.config.producer.compression_threshold,
        required_acks: ::WaterDrop.config.required_acks
      }
    end

    before do
      WaterDrop.config.kafka.seed_brokers = kafka

      expect(producer_proxy)
        .to receive(:dead?)
        .and_return(dead)
    end

    context 'when producer is dead' do
      let(:dead) { true }

      before do
        expect(producer_proxy).to receive(:reload!)

        expect(Kafka)
          .to receive(:new)
          .with(kafka_init_parameters).and_return(kafka)
      end

      it 'expect to reload and create producer' do
        expect(kafka).to receive(:producer).with(producer_init_parameters)
        producer_proxy.send :producer
      end
    end

    context 'when producer is not dead' do
      let(:dead) { false }

      before do
        expect(producer_proxy)
          .not_to receive(:reload!)

        expect(Kafka)
          .to receive(:new)
          .with(kafka_init_parameters)
          .and_return(kafka)
      end

      it 'expect not to reload and create producer' do
        expect(kafka).to receive(:producer)

        producer_proxy.send :producer
      end
    end
  end

  describe '#dead?' do
    before do
      producer_proxy.instance_variable_set(:@last_usage, last_usage)
    end

    context 'when we didnt exceed life time' do
      let(:last_usage) { Time.now - described_class::LIFE_TIME + 1 }

      it 'expect not to be dead' do
        expect(producer_proxy.send(:dead?)).to eq false
      end
    end

    context 'when we did exceed life time' do
      let(:last_usage) { Time.now - described_class::LIFE_TIME - 1 }

      it 'expect to be dead' do
        expect(producer_proxy.send(:dead?)).to eq true
      end
    end
  end

  describe '#reload!' do
    let(:producer) { double }

    before do
      producer_proxy.instance_variable_set(:@producer, producer)
    end

    it 'expect to shutdown producer and set it to nil' do
      expect(producer)
        .to receive(:shutdown)

      producer_proxy.send :reload!

      expect(producer_proxy.instance_variable_get(:@producer)).to eq nil
    end
  end

  context 'class methods' do
    subject(:klass) { described_class }

    describe '.new' do
      it 'expect to touch after initializing' do
        instance = klass.new
        expect(instance.instance_variable_get('@last_usage')).not_to be_nil
      end
    end
  end
end
