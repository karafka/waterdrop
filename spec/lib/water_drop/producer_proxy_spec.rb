require 'spec_helper'

RSpec.describe WaterDrop::ProducerProxy do
  subject { described_class }

  describe '.new' do
    it 'expect to touch after initializing' do
      instance = subject.new
      expect(instance.instance_variable_get('@last_usage')).not_to be_nil
    end
  end

  describe described_class do
    subject { described_class.new }

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
          expect(subject)
            .to receive(:producer)
            .and_return(producer)
            .exactly(2).times

          expect(subject)
            .to receive(:touch)
        end

        it 'expect to touch and forward to producer' do
          expect(producer)
            .to receive(:produce)
            .with(message.message, topic: message.topic)

          expect(producer).to receive(:deliver_messages)

          subject.send_message(message)
        end

        context 'with optional producer arguments' do
          let(:message_options) { { partition: rand, partition_key: rand } }

          it 'expect to forward to producer' do
            expect(producer)
              .to receive(:produce)
              .with(message.message, { topic: message.topic }.merge(message_options))

            expect(producer).to receive(:deliver_messages)

            subject.send_message(message)
          end
        end
      end

      context 'when something went wrong' do
        let(:error) { StandardError }

        before do
          expect(subject)
            .to receive(:producer)
            .and_return(producer)
            .exactly(2).times

          expect(subject)
            .to receive(:touch)
            .exactly(2).times

          expect(producer)
            .to receive(:produce)
            .with(message.message, topic: message.topic)
            .and_raise(error)
            .exactly(2).times
        end

        it 'expect to reload producer retry once and if fails again reraise error' do
          expect(subject)
            .to receive(:reload!)
            .exactly(2).times

          expect { subject.send_message(message) }.to raise_error(error)
        end
      end
    end

    describe '#touch' do
      let(:now) { rand }

      before do
        subject

        expect(Time)
          .to receive(:now)
          .and_return(now)
      end

      it 'expect to update time to time now' do
        subject.send :touch
        expect(subject.instance_variable_get(:@last_usage)).to eq now
      end
    end

    describe '#producer' do
      let(:kafka) { double }

      before do
        WaterDrop.config.kafka.hosts = kafka

        expect(subject)
          .to receive(:dead?)
          .and_return(dead)
      end

      context 'when producer is dead' do
        let(:dead) { true }

        before do
          expect(subject).to receive(:reload!)

          expect(Kafka)
            .to receive(:new)
            .with(
              seed_brokers: ::WaterDrop.config.kafka.hosts
            ).and_return(kafka)
        end

        it 'expect to reload and create producer' do
          expect(kafka).to receive(:producer)
          subject.send :producer
        end
      end

      context 'when producer is not dead' do
        let(:dead) { false }

        before do
          expect(subject)
            .not_to receive(:reload!)

          expect(Kafka)
            .to receive(:new)
            .with(
              seed_brokers: ::WaterDrop.config.kafka.hosts
            )
            .and_return(kafka)
        end

        it 'expect not to reload and create producer' do
          expect(kafka).to receive(:producer)

          subject.send :producer
        end
      end
    end

    describe '#dead?' do
      before do
        subject.instance_variable_set(:@last_usage, last_usage)
      end

      context 'when we didnt exceed life time' do
        let(:last_usage) { Time.now - described_class::LIFE_TIME + 1 }

        it 'expect not to be dead' do
          expect(subject.send(:dead?)).to eq false
        end
      end

      context 'when we did excee life time' do
        let(:last_usage) { Time.now - described_class::LIFE_TIME - 1 }

        it 'expect to be dead' do
          expect(subject.send(:dead?)).to eq true
        end
      end
    end

    describe '#reload!' do
      let(:producer) { double }

      before do
        subject.instance_variable_set(:@producer, producer)
      end

      it 'expect to shutdown producer and set it to nil' do
        expect(producer)
          .to receive(:shutdown)

        subject.send :reload!

        expect(subject.instance_variable_get(:@producer)).to eq nil
      end
    end
  end
end
