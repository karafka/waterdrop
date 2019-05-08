# frozen_string_literal: true

RSpec.describe WaterDrop::Schemas::MessageOptions do
  let(:schema) { described_class.new }

  let(:message_options) do
    {
      topic: 'name',
      key: rand.to_s,
      partition: 0,
      partition_key: rand.to_s,
      create_time: Time.now,
      headers: {}
    }
  end

  context 'when message_options are valid' do
    it { expect(schema.call(message_options)).to be_success }
  end

  context 'when we run topic validations' do
    context 'when topic is nil but present in options' do
      before { message_options[:topic] = nil }

      it { expect(schema.call(message_options)).not_to be_success }
    end

    context 'when topic is not a string' do
      before { message_options[:topic] = rand }

      it { expect(schema.call(message_options)).not_to be_success }
    end

    context 'when topic is a symbol' do
      before { message_options[:topic] = :symbol }

      it { expect(schema.call(message_options)).not_to be_success }
    end

    context 'when topic has an invalid format' do
      before { message_options[:topic] = '%^&*(' }

      it { expect(schema.call(message_options)).not_to be_success }
    end

    context 'when topic is not present in options' do
      before { message_options.delete(:topic) }

      it { expect(schema.call(message_options)).not_to be_success }
    end
  end

  context 'when we run key validations' do
    context 'when key is nil but present in options' do
      before { message_options[:key] = nil }

      it { expect(schema.call(message_options)).to be_success }
    end

    context 'when key is not a string' do
      before { message_options[:key] = rand }

      it { expect(schema.call(message_options)).not_to be_success }
    end

    context 'when key is empty' do
      before { message_options[:key] = '' }

      it { expect(schema.call(message_options)).not_to be_success }
    end

    context 'when key is valid' do
      before { message_options[:key] = rand.to_s }

      it { expect(schema.call(message_options)).to be_success }
    end

    context 'when key is not present in options' do
      before { message_options.delete(:key) }

      it { expect(schema.call(message_options)).to be_success }
    end
  end

  context 'when we run partition validations' do
    context 'when partition is nil but present in options' do
      before { message_options[:partition] = nil }

      it { expect(schema.call(message_options)).not_to be_success }
    end

    context 'when partition is not an int' do
      before { message_options[:partition] = rand }

      it { expect(schema.call(message_options)).not_to be_success }
    end

    context 'when partition is empty' do
      before { message_options[:partition] = '' }

      it { expect(schema.call(message_options)).not_to be_success }
    end

    context 'when partition is valid' do
      before { message_options[:partition] = rand(100) }

      it { expect(schema.call(message_options)).to be_success }
    end

    context 'when partition is not present in options' do
      before { message_options.delete(:partition) }

      it { expect(schema.call(message_options)).to be_success }
    end

    context 'when partition is less than 0' do
      before { message_options[:partition] = rand(1..100) * -1 }

      it { expect(schema.call(message_options)).not_to be_success }
    end
  end

  context 'when we run partition_key validations' do
    context 'when partition_key is nil but present in options' do
      before { message_options[:partition_key] = nil }

      it { expect(schema.call(message_options)).to be_success }
    end

    context 'when partition_key is not a string' do
      before { message_options[:partition_key] = rand }

      it { expect(schema.call(message_options)).not_to be_success }
    end

    context 'when partition_key is empty' do
      before { message_options[:partition_key] = '' }

      it { expect(schema.call(message_options)).not_to be_success }
    end

    context 'when partition_key is valid' do
      before { message_options[:partition_key] = rand.to_s }

      it { expect(schema.call(message_options)).to be_success }
    end

    context 'when partition_key is not present in options' do
      before { message_options.delete(:partition_key) }

      it { expect(schema.call(message_options)).to be_success }
    end
  end

  context 'when we run create_time validations' do
    context 'when create_time is nil but present in options' do
      before { message_options[:create_time] = nil }

      it { expect(schema.call(message_options)).to be_success }
    end

    context 'when create_time is not a time' do
      before { message_options[:create_time] = rand }

      it { expect(schema.call(message_options)).not_to be_success }
    end

    context 'when create_time is empty' do
      before { message_options[:create_time] = '' }

      it { expect(schema.call(message_options)).not_to be_success }
    end

    context 'when create_time is valid' do
      before { message_options[:create_time] = Time.now }

      it { expect(schema.call(message_options)).to be_success }
    end

    context 'when create_time is not present in options' do
      before { message_options.delete(:create_time) }

      it { expect(schema.call(message_options)).to be_success }
    end
  end

  context 'when we run headers validations' do
    context 'when headers is nil but present in options' do
      before { message_options[:headers] = nil }

      it { expect(schema.call(message_options)).to be_success }
    end

    context 'when headers is not a hash' do
      before { message_options[:headers] = rand }

      it { expect(schema.call(message_options)).not_to be_success }
    end

    context 'when headers is an empty hash' do
      before { message_options[:headers] = {} }

      it { expect(schema.call(message_options)).to be_success }
    end

    context 'when headers is valid hash with data' do
      before { message_options[:headers] = { rand.to_s => rand.to_s } }

      it { expect(schema.call(message_options)).to be_success }
    end

    context 'when headers is not present in options' do
      before { message_options.delete(:headers) }

      it { expect(schema.call(message_options)).to be_success }
    end
  end
end
