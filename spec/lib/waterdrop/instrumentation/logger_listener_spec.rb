# frozen_string_literal: true

RSpec.describe_current do
  subject(:logged_data) { buffer.tap(&:rewind).read.split("\n") }

  let(:listener) { described_class.new(logger) }
  let(:event) { Karafka::Core::Monitoring::Event.new('event', details) }
  let(:logger) { Logger.new(buffer) }
  let(:buffer) { StringIO.new }
  let(:producer) { build(:producer) }
  let(:message) { build(:valid_message) }
  let(:messages) { [message, build(:valid_message)] }
  let(:details) do
    {
      message: message,
      messages: messages,
      producer_id: producer.id,
      time: rand(100),
      error: Rdkafka::RdkafkaError,
      dispatched: [messages[0]]
    }
  end

  after { producer.close }

  describe '#on_message_produced_async' do
    before { listener.on_message_produced_async(event) }

    it { expect(logged_data[0]).to include(producer.id) }
    it { expect(logged_data[0]).to include('INFO') }
    it { expect(logged_data[0]).to include('Async producing of a message to') }
    it { expect(logged_data[0]).to include(message[:topic]) }
    it { expect(logged_data[1]).to include(producer.id) }
    it { expect(logged_data[1]).to include('DEBUG') }
    it { expect(logged_data[1]).to include(message.to_s) }

    context 'when we do not want to log messages content' do
      let(:listener) { described_class.new(logger, log_messages: false) }

      it { expect(logged_data[0]).to include(producer.id) }
      it { expect(logged_data[0]).to include('INFO') }
      it { expect(logged_data[0]).to include('Async producing of a message to') }
      it { expect(logged_data[0]).to include(message[:topic]) }
      it { expect(logged_data[1]).to eq(nil) }
    end
  end

  describe '#on_message_produced_sync' do
    before { listener.on_message_produced_sync(event) }

    it { expect(logged_data[0]).to include(producer.id) }
    it { expect(logged_data[0]).to include('INFO') }
    it { expect(logged_data[0]).to include('Sync producing of a message to') }
    it { expect(logged_data[0]).to include(message[:topic]) }
    it { expect(logged_data[1]).to include(producer.id) }
    it { expect(logged_data[1]).to include('DEBUG') }
    it { expect(logged_data[1]).to include(message.to_s) }

    context 'when we do not want to log messages content' do
      let(:listener) { described_class.new(logger, log_messages: false) }

      it { expect(logged_data[0]).to include(producer.id) }
      it { expect(logged_data[0]).to include('INFO') }
      it { expect(logged_data[0]).to include('Sync producing of a message to') }
      it { expect(logged_data[0]).to include(message[:topic]) }
      it { expect(logged_data[1]).to eq(nil) }
    end
  end

  describe '#on_messages_produced_async' do
    before { listener.on_messages_produced_async(event) }

    it { expect(logged_data[0]).to include(producer.id) }
    it { expect(logged_data[0]).to include('INFO') }
    it { expect(logged_data[0]).to include('Async producing of 2 messages to 2 topics') }
    it { expect(logged_data[1]).to include(producer.id) }
    it { expect(logged_data[1]).to include('DEBUG') }
    it { expect(logged_data[1]).to include(message.to_s) }

    context 'when we do not want to log messages content' do
      let(:listener) { described_class.new(logger, log_messages: false) }

      it { expect(logged_data[0]).to include(producer.id) }
      it { expect(logged_data[0]).to include('INFO') }
      it { expect(logged_data[0]).to include('Async producing of 2 messages to 2 topics') }
      it { expect(logged_data[1]).to eq(nil) }
    end
  end

  describe '#on_messages_produced_sync' do
    before { listener.on_messages_produced_sync(event) }

    it { expect(logged_data[0]).to include(producer.id) }
    it { expect(logged_data[0]).to include('INFO') }
    it { expect(logged_data[0]).to include('Sync producing of 2 messages to 2 topics') }
    it { expect(logged_data[1]).to include(producer.id) }
    it { expect(logged_data[1]).to include('DEBUG') }
    it { expect(logged_data[1]).to include(message.to_s) }

    context 'when we do not want to log messages content' do
      let(:listener) { described_class.new(logger, log_messages: false) }

      it { expect(logged_data[0]).to include(producer.id) }
      it { expect(logged_data[0]).to include('INFO') }
      it { expect(logged_data[0]).to include('Sync producing of 2 messages to 2 topics') }
      it { expect(logged_data[1]).to eq(nil) }
    end
  end

  describe '#on_message_buffered' do
    before { listener.on_message_buffered(event) }

    it { expect(logged_data[0]).to include(producer.id) }
    it { expect(logged_data[0]).to include('INFO') }
    it { expect(logged_data[0]).to include('Buffering of a message to ') }
    it { expect(logged_data[0]).to include(message[:topic]) }
    it { expect(logged_data[1]).to include(producer.id) }
    it { expect(logged_data[1]).to include('DEBUG') }
    it { expect(logged_data[1]).to include(message.to_s) }

    context 'when we do not want to log messages content' do
      let(:listener) { described_class.new(logger, log_messages: false) }

      it { expect(logged_data[0]).to include(producer.id) }
      it { expect(logged_data[0]).to include('INFO') }
      it { expect(logged_data[0]).to include('Buffering of a message to ') }
      it { expect(logged_data[0]).to include(message[:topic]) }
      it { expect(logged_data[1]).to eq(nil) }
    end
  end

  describe '#on_messages_buffered' do
    before { listener.on_messages_buffered(event) }

    it { expect(logged_data[0]).to include(producer.id) }
    it { expect(logged_data[0]).to include('INFO') }
    it { expect(logged_data[0]).to include('Buffering of 2 messages ') }
    it { expect(logged_data[1]).to include(producer.id) }
    it { expect(logged_data[1]).to include('DEBUG') }
    it { expect(logged_data[1]).to include(message.to_s) }

    context 'when we do not want to log messages content' do
      let(:listener) { described_class.new(logger, log_messages: false) }

      it { expect(logged_data[0]).to include(producer.id) }
      it { expect(logged_data[0]).to include('INFO') }
      it { expect(logged_data[0]).to include('Buffering of 2 messages ') }
      it { expect(logged_data[1]).to eq(nil) }
    end
  end

  describe '#on_buffer_flushed_async' do
    before { listener.on_buffer_flushed_async(event) }

    it { expect(logged_data[0]).to include(producer.id) }
    it { expect(logged_data[0]).to include('INFO') }
    it { expect(logged_data[0]).to include('Async flushing of 2 messages from the buffer') }
    it { expect(logged_data[1]).to include(producer.id) }
    it { expect(logged_data[1]).to include('DEBUG') }
    it { expect(logged_data[1]).to include(message.to_s) }
    it { expect(logged_data[1]).to include(messages[0].to_s) }
    it { expect(logged_data[1]).to include(messages[1].to_s) }

    context 'when we do not want to log messages content' do
      let(:listener) { described_class.new(logger, log_messages: false) }

      it { expect(logged_data[0]).to include(producer.id) }
      it { expect(logged_data[0]).to include('INFO') }
      it { expect(logged_data[0]).to include('Async flushing of 2 messages from the buffer') }
      it { expect(logged_data[1]).to eq(nil) }
    end
  end

  describe '#on_buffer_flushed_sync' do
    before { listener.on_buffer_flushed_sync(event) }

    it { expect(logged_data[0]).to include(producer.id) }
    it { expect(logged_data[0]).to include('INFO') }
    it { expect(logged_data[0]).to include('Sync flushing of 2 messages from the buffer') }
    it { expect(logged_data[1]).to include(producer.id) }
    it { expect(logged_data[1]).to include('DEBUG') }
    it { expect(logged_data[1]).to include(messages[0].to_s) }
    it { expect(logged_data[1]).to include(messages[1].to_s) }

    context 'when we do not want to log messages content' do
      let(:listener) { described_class.new(logger, log_messages: false) }

      it { expect(logged_data[0]).to include(producer.id) }
      it { expect(logged_data[0]).to include('INFO') }
      it { expect(logged_data[0]).to include('Sync flushing of 2 messages from the buffer') }
      it { expect(logged_data[1]).to eq(nil) }
    end
  end

  describe '#on_buffer_purged' do
    before { listener.on_buffer_purged(event) }

    it { expect(logged_data[0]).to include(producer.id) }
    it { expect(logged_data[0]).to include('INFO') }
    it { expect(logged_data[0]).to include('Successfully purging buffer') }
    it { expect(logged_data[1]).to eq(nil) }
  end

  describe '#on_producer_closed' do
    before { listener.on_producer_closed(event) }

    it { expect(logged_data[0]).to include(producer.id) }
    it { expect(logged_data[0]).to include('INFO') }
    it { expect(logged_data[0]).to include('Closing producer') }
  end

  describe '#on_error_occurred' do
    before do
      details[:type] = 'error.type'
      listener.on_error_occurred(event)
    end

    it { expect(logged_data[0]).to include(producer.id) }
    it { expect(logged_data[0]).to include('ERROR') }
    it { expect(logged_data[0]).to include('Error occurred') }
  end

  describe '#on_transaction_started' do
    before { listener.on_transaction_started(event) }

    it { expect(logged_data[0]).to include(producer.id) }
    it { expect(logged_data[0]).to include('INFO') }
    it { expect(logged_data[0]).to include('Starting transaction') }
  end

  describe '#on_transaction_aborted' do
    before { listener.on_transaction_aborted(event) }

    it { expect(logged_data[0]).to include(producer.id) }
    it { expect(logged_data[0]).to include('INFO') }
    it { expect(logged_data[0]).to include('Aborting transaction') }
  end

  describe '#on_transaction_committed' do
    before { listener.on_transaction_committed(event) }

    it { expect(logged_data[0]).to include(producer.id) }
    it { expect(logged_data[0]).to include('INFO') }
    it { expect(logged_data[0]).to include('Committing transaction') }
  end

  describe '#on_transaction_offset_stored' do
    before do
      details[:topic] = rand.to_s
      details[:partition] = 0
      details[:offset] = 100

      listener.on_transaction_offset_stored(event)
    end

    it { expect(logged_data[0]).to include(producer.id) }
    it { expect(logged_data[0]).to include('INFO') }
    it { expect(logged_data[0]).to include('Storing offset') }
  end
end
