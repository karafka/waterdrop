require 'spec_helper'

RSpec.describe WaterDrop::ProducerProxy do
  subject { described_class }

  describe '.new' do
    it 'should touch after initializing' do
      expect_any_instance_of(subject)
        .to receive(:touch)

      subject.new
    end
  end
end

RSpec.describe WaterDrop::ProducerProxy do
  subject { described_class.new }

  describe '#send_message' do
    let(:producer) { double }
    let(:message) { double(message: rand, topic: rand) }

    context 'when sending was successful (no errors)' do
      before do
        expect(subject)
          .to receive(:producer)
          .and_return(producer)
          .exactly(2).times

        expect(subject)
          .to receive(:touch)
      end

      it 'should touch and forward to producer' do
        expect(producer)
          .to receive(:produce)
          .with(message.message, topic: message.topic)

        expect(producer)
          .to receive(:deliver_messages)

        subject.send_message(message)
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
      end

      it 'should reload producer retry once and if fails again reraise error' do
        expect(producer)
          .to receive(:produce)
          .with(message.message, topic: message.topic)
          .and_raise(error)
          .exactly(2).times

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

    it 'should update time to time now' do
      subject.send :touch
      expect(subject.instance_variable_get(:@last_usage)).to eq now
    end
  end

  describe '#producer' do
    let(:kafka) { double }

    before do
      expect(subject)
        .to receive(:dead?)
        .and_return(dead)
    end

    context 'when producer is dead' do
      let(:dead) { true }

      it 'should reload and create producer' do
        expect(subject)
          .to receive(:reload!)

        expect(Kafka)
          .to receive(:new)
          .with(
            seed_brokers: ::WaterDrop.config.kafka_hosts
          ).and_return(kafka)

        expect(kafka)
          .to receive(:producer)

        subject.send :producer
      end
    end

    context 'when producer is not dead' do
      let(:dead) { false }

      it 'should not reload and create producer' do
        expect(subject)
          .not_to receive(:reload!)

        expect(Kafka)
          .to receive(:new)
          .with(
            seed_brokers: ::WaterDrop.config.kafka_hosts
          )
          .and_return(kafka)

        expect(kafka)
          .to receive(:producer)

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

      it 'should not be dead' do
        expect(subject.send(:dead?)).to eq false
      end
    end

    context 'when we did excee life time' do
      let(:last_usage) { Time.now - described_class::LIFE_TIME - 1 }

      it 'should be dead' do
        expect(subject.send(:dead?)).to eq true
      end
    end
  end

  describe '#reload!' do
    let(:producer) { double }

    before do
      subject.instance_variable_set(:@producer, producer)
    end

    it 'should shutdown producer and set it to nil' do
      expect(producer)
        .to receive(:shutdown)

      subject.send :reload!

      expect(subject.instance_variable_get(:@producer)).to eq nil
    end
  end
end
