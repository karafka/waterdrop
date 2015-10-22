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

  describe '#send_messages' do
    let(:producer) { double }
    let(:messages) { double }

    it 'should touch and forward to producer' do
      expect(subject)
        .to receive(:touch)

      expect(subject)
        .to receive(:producer)
        .and_return(producer)

      expect(producer)
        .to receive(:send_messages)
        .with(messages)

      subject.send_messages(messages)
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
    before do
      expect(subject)
        .to receive(:dead?)
        .and_return(dead)
    end

    let(:producer_id) { rand }

    context 'when producer is dead' do
      let(:dead) { true }

      it 'should reload and create producer' do
        expect(subject)
          .to receive(:reload!)

        expect(subject)
          .to receive(:producer_id)
          .and_return(producer_id)

        expect(Poseidon::Producer)
          .to receive(:new)
          .with(
            ::WaterDrop.config.kafka_hosts,
            producer_id,
            metadata_refresh_interval_ms: described_class::METADATA_REFRESH_INTERVAL * 1000,
            required_acks: described_class::REQUIRED_ACKS
          )

        subject.send :producer
      end
    end

    context 'when producer is not dead' do
      let(:dead) { false }

      it 'should not reload and create producer' do
        expect(subject)
          .not_to receive(:reload!)

        expect(subject)
          .to receive(:producer_id)
          .and_return(producer_id)

        expect(Poseidon::Producer)
          .to receive(:new)
          .with(
            ::WaterDrop.config.kafka_hosts,
            producer_id,
            metadata_refresh_interval_ms: described_class::METADATA_REFRESH_INTERVAL * 1000,
            required_acks: described_class::REQUIRED_ACKS
          )

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

    it 'should set producer to nil' do
      subject.send :reload!
      expect(subject.instance_variable_get(:@producer)).to eq nil
    end
  end

  describe '#producer_id' do
    let(:now) { rand }

    it 'should build it based on subject id and time' do
      expect(Time)
        .to receive(:now)
        .and_return(now)
        .exactly(2).times

      expect(subject.send(:producer_id)).to eq subject.object_id.to_s + now.to_f.to_s
    end
  end
end
