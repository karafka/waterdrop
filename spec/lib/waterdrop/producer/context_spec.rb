# frozen_string_literal: true

RSpec.describe_current do
  subject(:producer) { build(:producer) }

  let(:config_error) { WaterDrop::Errors::ContextInvalidError }
  let(:produce_error) { WaterDrop::Errors::ProduceError }
  let(:rd_config_error) { Rdkafka::Config::ConfigError }

  after { producer.close }

  context 'when trying to create context with invalid string keys setup' do
    it 'expect to raise the correct error' do
      expect do
        producer.with(topic_config: { 'acks' => 1 })
      end.to raise_error(config_error)
    end
  end

  context 'when trying to create context with attribute that is not per topic' do
    it 'expect to raise the correct error' do
      expect do
        producer.with(topic_config: { 'batch.size': 1 })
      end.to raise_error(config_error)
    end
  end

  context 'when having altered producer with a config with invalid value' do
    let(:altered) { producer.with(topic_config: { 'message.timeout.ms': -1_000 }) }

    it 'expect to raise error' do
      expect { altered.produce_sync(topic: 'test', payload: '') }.to raise_error(rd_config_error)
    end

    it 'expect to allow the original one' do
      altered
      expect { producer.produce_sync(topic: SecureRandom.uuid, payload: '') }.not_to raise_error
    end
  end

  context 'when having altered producer with a valid value' do
    let(:altered) { producer.with(topic_config: { 'message.timeout.ms': 100_000 }) }

    it 'expect to work' do
      expect { altered.produce_sync(topic: SecureRandom.uuid, payload: '') }.not_to raise_error
    end
  end

  context 'when having a producer with altered max_wait_timeout' do
    # 1 ms will always be not enough to dispatch to a topic that needs to be created
    let(:altered) { producer.with(max_wait_timeout: 1) }

    it 'expect to use this timeout it' do
      expect do
        altered.produce_sync(topic: SecureRandom.uuid, payload: '')
      end.to raise_error(produce_error)
    end
  end

  context 'when having a transactional producer with alterations' do
    subject(:producer) { build(:transactional_producer) }

    let(:altered) { producer.with(topic_config: { 'message.timeout.ms': 10_000 }) }

    it 'expect to use the settings' do
      expect do
        altered.produce_sync(topic: SecureRandom.uuid, payload: '')
      end.not_to raise_error
    end

    context 'when trying to overwrite acks on a transactional producer' do
      let(:altered) { producer.with(topic_config: { acks: 1 }) }

      it 'expect not to allow it' do
        expect do
          altered.produce_sync(topic: SecureRandom.uuid, payload: '')
        end.to raise_error(config_error)
      end
    end
  end
end
