# frozen_string_literal: true

RSpec.describe_current do
  subject(:producer) { build(:producer) }

  let(:config_error) { WaterDrop::Errors::VariantInvalidError }
  let(:produce_error) { WaterDrop::Errors::ProduceError }
  let(:rd_config_error) { Rdkafka::Config::ConfigError }
  let(:topic) { "it-#{SecureRandom.uuid}" }

  after { producer.close }

  describe "#variant" do
    it "expect to allow to create variants" do
      expect { producer.variant(topic_config: { acks: 1 }) }.not_to raise_error
    end
  end

  context "when referencing original producer back" do
    let(:variant) { producer.with(topic_config: { acks: 1 }) }

    it "expect to be able to reference it" do
      expect(variant.producer).to eq(producer)
    end
  end

  context "when trying to create variant with invalid string keys setup" do
    it "expect to raise the correct error" do
      expect do
        producer.with(topic_config: { "acks" => 1 })
      end.to raise_error(config_error)
    end
  end

  context "when trying to create variant with attribute that is not per topic" do
    it "expect to raise the correct error" do
      expect do
        producer.with(topic_config: { "batch.size": 1 })
      end.to raise_error(config_error)
    end
  end

  context "when having variant producer with a config with invalid value" do
    let(:variant) { producer.with(topic_config: { "message.timeout.ms": -1_000 }) }

    it "expect to raise error" do
      expect { variant.produce_sync(topic: "test", payload: "") }.to raise_error(rd_config_error)
    end

    it "expect to allow the original one" do
      variant
      expect { producer.produce_sync(topic: topic, payload: "") }.not_to raise_error
    end
  end

  context "when having variant producer with a valid value" do
    let(:variant) { producer.with(topic_config: { "message.timeout.ms": 100_000 }) }

    it "expect to work" do
      expect { variant.produce_sync(topic: topic, payload: "") }.not_to raise_error
    end
  end

  context "when having a producer with variant max_wait_timeout" do
    # 1 ms will always be not enough to dispatch to a topic that needs to be created
    let(:variant) { producer.with(max_wait_timeout: 1) }

    it "expect to use this timeout it" do
      expect do
        variant.produce_sync(topic: topic, payload: "")
      end.to raise_error(produce_error)
    end
  end

  context "when having a transactional producer with alterations" do
    subject(:producer) { build(:transactional_producer) }

    let(:variant) { producer.with(topic_config: { "message.timeout.ms": 10_000 }) }

    it "expect to use the settings" do
      expect do
        variant.produce_sync(topic: topic, payload: "")
      end.not_to raise_error
    end

    context "when trying to overwrite acks on a transactional producer" do
      let(:variant) { producer.with(topic_config: { acks: 1 }) }

      it "expect not to allow it" do
        expect do
          variant.produce_sync(topic: topic, payload: "")
        end.to raise_error(config_error)
      end
    end
  end

  context "when having an idempotent producer with alterations" do
    subject(:producer) { build(:idempotent_producer) }

    let(:variant) { producer.with(topic_config: { "message.timeout.ms": 10_000 }) }

    it "expect to use the settings" do
      expect do
        variant.produce_sync(topic: topic, payload: "")
      end.not_to raise_error
    end

    context "when trying to overwrite acks on a idempotent producer" do
      let(:variant) { producer.with(topic_config: { acks: 1 }) }

      it "expect not to allow it" do
        expect do
          variant.produce_sync(topic: topic, payload: "")
        end.to raise_error(config_error)
      end
    end
  end

  context "when trying to lower the acks on an idempotent producer" do
    subject(:variant) { producer.variant(topic_config: { acks: 1 }) }

    let(:producer) { build(:idempotent_producer) }

    it "expect not to allow it" do
      expect do
        variant.produce_sync(topic: topic, payload: "")
      end.to raise_error(config_error)
    end
  end
end
