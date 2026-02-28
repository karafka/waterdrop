# frozen_string_literal: true

class WaterDropInstrumentationLoggerListenerTest < WaterDropTest::Base
  def setup
    super
    @buffer = StringIO.new
    @logger = Logger.new(@buffer)
    @producer = build(:producer)
    @message = build(:valid_message)
    @messages = [@message, build(:valid_message)]
    @details = {
      message: @message,
      messages: @messages,
      producer_id: @producer.id,
      time: rand(100),
      error: Rdkafka::RdkafkaError,
      dispatched: [@messages[0]]
    }
    @event = Karafka::Core::Monitoring::Event.new("event", @details)
    @listener = WaterDrop::Instrumentation::LoggerListener.new(@logger)
  end

  def teardown
    @producer.close
    super
  end

  private

  def logged_data
    @buffer.tap(&:rewind).read.split("\n")
  end
end

class WaterDropInstrumentationLoggerListenerOnMessageProducedAsyncTest <
  WaterDropInstrumentationLoggerListenerTest
  def setup
    super
    @listener.on_message_produced_async(@event)
  end

  def test_includes_producer_id
    assert_includes logged_data[0], @producer.id
  end

  def test_includes_info_level
    assert_includes logged_data[0], "INFO"
  end

  def test_includes_dispatch_queue_message
    assert_includes logged_data[0], "topic was delegated to a dispatch queue"
  end

  def test_includes_topic_name
    assert_includes logged_data[0], @message[:topic]
  end

  def test_debug_line_includes_producer_id
    assert_includes logged_data[1], @producer.id
  end

  def test_debug_line_includes_debug_level
    assert_includes logged_data[1], "DEBUG"
  end

  def test_debug_line_includes_message_content
    assert_includes logged_data[1], @message.to_s
  end
end

class WaterDropInstrumentationLoggerListenerOnMessageProducedAsyncNoLogMessagesTest <
  WaterDropInstrumentationLoggerListenerTest
  def setup
    super
    @listener = WaterDrop::Instrumentation::LoggerListener.new(@logger, log_messages: false)
    @listener.on_message_produced_async(@event)
  end

  def test_includes_producer_id
    assert_includes logged_data[0], @producer.id
  end

  def test_includes_info_level
    assert_includes logged_data[0], "INFO"
  end

  def test_includes_dispatch_queue_message
    assert_includes logged_data[0], "topic was delegated to a dispatch queue"
  end

  def test_includes_topic_name
    assert_includes logged_data[0], @message[:topic]
  end

  def test_no_debug_line
    assert_nil logged_data[1]
  end
end

class WaterDropInstrumentationLoggerListenerOnMessageProducedSyncTest <
  WaterDropInstrumentationLoggerListenerTest
  def setup
    super
    @listener.on_message_produced_sync(@event)
  end

  def test_includes_producer_id
    assert_includes logged_data[0], @producer.id
  end

  def test_includes_info_level
    assert_includes logged_data[0], "INFO"
  end

  def test_includes_sync_producing_message
    assert_includes logged_data[0], "Sync producing of a message to"
  end

  def test_includes_topic_name
    assert_includes logged_data[0], @message[:topic]
  end

  def test_debug_line_includes_producer_id
    assert_includes logged_data[1], @producer.id
  end

  def test_debug_line_includes_debug_level
    assert_includes logged_data[1], "DEBUG"
  end

  def test_debug_line_includes_message_content
    assert_includes logged_data[1], @message.to_s
  end
end

class WaterDropInstrumentationLoggerListenerOnMessageProducedSyncNoLogMessagesTest <
  WaterDropInstrumentationLoggerListenerTest
  def setup
    super
    @listener = WaterDrop::Instrumentation::LoggerListener.new(@logger, log_messages: false)
    @listener.on_message_produced_sync(@event)
  end

  def test_includes_producer_id
    assert_includes logged_data[0], @producer.id
  end

  def test_includes_info_level
    assert_includes logged_data[0], "INFO"
  end

  def test_includes_sync_producing_message
    assert_includes logged_data[0], "Sync producing of a message to"
  end

  def test_includes_topic_name
    assert_includes logged_data[0], @message[:topic]
  end

  def test_no_debug_line
    assert_nil logged_data[1]
  end
end

class WaterDropInstrumentationLoggerListenerOnMessagesProducedAsyncTest <
  WaterDropInstrumentationLoggerListenerTest
  def setup
    super
    @dispatched_msg = "2 messages to 2 topics were delegated to a dispatch queue"
    @listener.on_messages_produced_async(@event)
  end

  def test_includes_producer_id
    assert_includes logged_data[0], @producer.id
  end

  def test_includes_info_level
    assert_includes logged_data[0], "INFO"
  end

  def test_includes_dispatched_message
    assert_includes logged_data[0], @dispatched_msg
  end

  def test_debug_line_includes_producer_id
    assert_includes logged_data[1], @producer.id
  end

  def test_debug_line_includes_debug_level
    assert_includes logged_data[1], "DEBUG"
  end

  def test_debug_line_includes_message_content
    assert_includes logged_data[1], @message.to_s
  end
end

class WaterDropInstrumentationLoggerListenerOnMessagesProducedAsyncNoLogMessagesTest <
  WaterDropInstrumentationLoggerListenerTest
  def setup
    super
    @dispatched_msg = "2 messages to 2 topics were delegated to a dispatch queue"
    @listener = WaterDrop::Instrumentation::LoggerListener.new(@logger, log_messages: false)
    @listener.on_messages_produced_async(@event)
  end

  def test_includes_producer_id
    assert_includes logged_data[0], @producer.id
  end

  def test_includes_info_level
    assert_includes logged_data[0], "INFO"
  end

  def test_includes_dispatched_message
    assert_includes logged_data[0], @dispatched_msg
  end

  def test_no_debug_line
    assert_nil logged_data[1]
  end
end

class WaterDropInstrumentationLoggerListenerOnMessagesProducedSyncTest <
  WaterDropInstrumentationLoggerListenerTest
  def setup
    super
    @listener.on_messages_produced_sync(@event)
  end

  def test_includes_producer_id
    assert_includes logged_data[0], @producer.id
  end

  def test_includes_info_level
    assert_includes logged_data[0], "INFO"
  end

  def test_includes_sync_producing_message
    assert_includes logged_data[0], "Sync producing of 2 messages to 2 topics"
  end

  def test_debug_line_includes_producer_id
    assert_includes logged_data[1], @producer.id
  end

  def test_debug_line_includes_debug_level
    assert_includes logged_data[1], "DEBUG"
  end

  def test_debug_line_includes_message_content
    assert_includes logged_data[1], @message.to_s
  end
end

class WaterDropInstrumentationLoggerListenerOnMessagesProducedSyncNoLogMessagesTest <
  WaterDropInstrumentationLoggerListenerTest
  def setup
    super
    @listener = WaterDrop::Instrumentation::LoggerListener.new(@logger, log_messages: false)
    @listener.on_messages_produced_sync(@event)
  end

  def test_includes_producer_id
    assert_includes logged_data[0], @producer.id
  end

  def test_includes_info_level
    assert_includes logged_data[0], "INFO"
  end

  def test_includes_sync_producing_message
    assert_includes logged_data[0], "Sync producing of 2 messages to 2 topics"
  end

  def test_no_debug_line
    assert_nil logged_data[1]
  end
end

class WaterDropInstrumentationLoggerListenerOnMessageBufferedTest <
  WaterDropInstrumentationLoggerListenerTest
  def setup
    super
    @listener.on_message_buffered(@event)
  end

  def test_includes_producer_id
    assert_includes logged_data[0], @producer.id
  end

  def test_includes_info_level
    assert_includes logged_data[0], "INFO"
  end

  def test_includes_buffering_message
    assert_includes logged_data[0], "Buffering of a message to "
  end

  def test_includes_topic_name
    assert_includes logged_data[0], @message[:topic]
  end

  def test_debug_line_includes_producer_id
    assert_includes logged_data[1], @producer.id
  end

  def test_debug_line_includes_debug_level
    assert_includes logged_data[1], "DEBUG"
  end

  def test_debug_line_includes_message_content
    assert_includes logged_data[1], @message.to_s
  end
end

class WaterDropInstrumentationLoggerListenerOnMessageBufferedNoLogMessagesTest <
  WaterDropInstrumentationLoggerListenerTest
  def setup
    super
    @listener = WaterDrop::Instrumentation::LoggerListener.new(@logger, log_messages: false)
    @listener.on_message_buffered(@event)
  end

  def test_includes_producer_id
    assert_includes logged_data[0], @producer.id
  end

  def test_includes_info_level
    assert_includes logged_data[0], "INFO"
  end

  def test_includes_buffering_message
    assert_includes logged_data[0], "Buffering of a message to "
  end

  def test_includes_topic_name
    assert_includes logged_data[0], @message[:topic]
  end

  def test_no_debug_line
    assert_nil logged_data[1]
  end
end

class WaterDropInstrumentationLoggerListenerOnMessagesBufferedTest <
  WaterDropInstrumentationLoggerListenerTest
  def setup
    super
    @listener.on_messages_buffered(@event)
  end

  def test_includes_producer_id
    assert_includes logged_data[0], @producer.id
  end

  def test_includes_info_level
    assert_includes logged_data[0], "INFO"
  end

  def test_includes_buffering_message
    assert_includes logged_data[0], "Buffering of 2 messages "
  end

  def test_debug_line_includes_producer_id
    assert_includes logged_data[1], @producer.id
  end

  def test_debug_line_includes_debug_level
    assert_includes logged_data[1], "DEBUG"
  end

  def test_debug_line_includes_message_content
    assert_includes logged_data[1], @message.to_s
  end
end

class WaterDropInstrumentationLoggerListenerOnMessagesBufferedNoLogMessagesTest <
  WaterDropInstrumentationLoggerListenerTest
  def setup
    super
    @listener = WaterDrop::Instrumentation::LoggerListener.new(@logger, log_messages: false)
    @listener.on_messages_buffered(@event)
  end

  def test_includes_producer_id
    assert_includes logged_data[0], @producer.id
  end

  def test_includes_info_level
    assert_includes logged_data[0], "INFO"
  end

  def test_includes_buffering_message
    assert_includes logged_data[0], "Buffering of 2 messages "
  end

  def test_no_debug_line
    assert_nil logged_data[1]
  end
end

class WaterDropInstrumentationLoggerListenerOnBufferFlushedAsyncTest <
  WaterDropInstrumentationLoggerListenerTest
  def setup
    super
    @listener.on_buffer_flushed_async(@event)
  end

  def test_includes_producer_id
    assert_includes logged_data[0], @producer.id
  end

  def test_includes_info_level
    assert_includes logged_data[0], "INFO"
  end

  def test_includes_async_flushing_message
    assert_includes logged_data[0], "Async flushing of 2 messages from the buffer"
  end

  def test_debug_line_includes_producer_id
    assert_includes logged_data[1], @producer.id
  end

  def test_debug_line_includes_debug_level
    assert_includes logged_data[1], "DEBUG"
  end

  def test_debug_line_includes_message_content
    assert_includes logged_data[1], @message.to_s
  end

  def test_debug_line_includes_first_message
    assert_includes logged_data[1], @messages[0].to_s
  end

  def test_debug_line_includes_second_message
    assert_includes logged_data[1], @messages[1].to_s
  end
end

class WaterDropInstrumentationLoggerListenerOnBufferFlushedAsyncNoLogMessagesTest <
  WaterDropInstrumentationLoggerListenerTest
  def setup
    super
    @listener = WaterDrop::Instrumentation::LoggerListener.new(@logger, log_messages: false)
    @listener.on_buffer_flushed_async(@event)
  end

  def test_includes_producer_id
    assert_includes logged_data[0], @producer.id
  end

  def test_includes_info_level
    assert_includes logged_data[0], "INFO"
  end

  def test_includes_async_flushing_message
    assert_includes logged_data[0], "Async flushing of 2 messages from the buffer"
  end

  def test_no_debug_line
    assert_nil logged_data[1]
  end
end

class WaterDropInstrumentationLoggerListenerOnBufferFlushedSyncTest <
  WaterDropInstrumentationLoggerListenerTest
  def setup
    super
    @listener.on_buffer_flushed_sync(@event)
  end

  def test_includes_producer_id
    assert_includes logged_data[0], @producer.id
  end

  def test_includes_info_level
    assert_includes logged_data[0], "INFO"
  end

  def test_includes_sync_flushing_message
    assert_includes logged_data[0], "Sync flushing of 2 messages from the buffer"
  end

  def test_debug_line_includes_producer_id
    assert_includes logged_data[1], @producer.id
  end

  def test_debug_line_includes_debug_level
    assert_includes logged_data[1], "DEBUG"
  end

  def test_debug_line_includes_first_message
    assert_includes logged_data[1], @messages[0].to_s
  end

  def test_debug_line_includes_second_message
    assert_includes logged_data[1], @messages[1].to_s
  end
end

class WaterDropInstrumentationLoggerListenerOnBufferFlushedSyncNoLogMessagesTest <
  WaterDropInstrumentationLoggerListenerTest
  def setup
    super
    @listener = WaterDrop::Instrumentation::LoggerListener.new(@logger, log_messages: false)
    @listener.on_buffer_flushed_sync(@event)
  end

  def test_includes_producer_id
    assert_includes logged_data[0], @producer.id
  end

  def test_includes_info_level
    assert_includes logged_data[0], "INFO"
  end

  def test_includes_sync_flushing_message
    assert_includes logged_data[0], "Sync flushing of 2 messages from the buffer"
  end

  def test_no_debug_line
    assert_nil logged_data[1]
  end
end

class WaterDropInstrumentationLoggerListenerOnBufferPurgedTest <
  WaterDropInstrumentationLoggerListenerTest
  def setup
    super
    @listener.on_buffer_purged(@event)
  end

  def test_includes_producer_id
    assert_includes logged_data[0], @producer.id
  end

  def test_includes_info_level
    assert_includes logged_data[0], "INFO"
  end

  def test_includes_purging_message
    assert_includes logged_data[0], "Successfully purging buffer"
  end

  def test_no_second_line
    assert_nil logged_data[1]
  end
end

class WaterDropInstrumentationLoggerListenerOnProducerClosingTest <
  WaterDropInstrumentationLoggerListenerTest
  def setup
    super
    @listener.on_producer_closing(@event)
  end

  def test_includes_producer_id
    assert_includes logged_data[0], @producer.id
  end

  def test_includes_info_level
    assert_includes logged_data[0], "INFO"
  end

  def test_includes_closing_message
    assert_includes logged_data[0], "Closing producer"
  end
end

class WaterDropInstrumentationLoggerListenerOnProducerClosedTest <
  WaterDropInstrumentationLoggerListenerTest
  def setup
    super
    @listener.on_producer_closed(@event)
  end

  def test_includes_producer_id
    assert_includes logged_data[0], @producer.id
  end

  def test_includes_info_level
    assert_includes logged_data[0], "INFO"
  end

  def test_includes_closing_message
    assert_includes logged_data[0], "Closing producer"
  end
end

class WaterDropInstrumentationLoggerListenerOnProducerDisconnectingTest <
  WaterDropInstrumentationLoggerListenerTest
  def setup
    super
    @listener.on_producer_disconnecting(@event)
  end

  def test_includes_producer_id
    assert_includes logged_data[0], @producer.id
  end

  def test_includes_info_level
    assert_includes logged_data[0], "INFO"
  end

  def test_includes_disconnecting_message
    assert_includes logged_data[0], "Disconnecting producer"
  end
end

class WaterDropInstrumentationLoggerListenerOnProducerDisconnectedTest <
  WaterDropInstrumentationLoggerListenerTest
  def setup
    super
    @listener.on_producer_disconnected(@event)
  end

  def test_includes_producer_id
    assert_includes logged_data[0], @producer.id
  end

  def test_includes_info_level
    assert_includes logged_data[0], "INFO"
  end

  def test_includes_disconnected_message
    assert_includes logged_data[0], "Disconnected producer"
  end
end

class WaterDropInstrumentationLoggerListenerOnProducerReloadedTest <
  WaterDropInstrumentationLoggerListenerTest
  def setup
    super
    @listener.on_producer_reloaded(@event)
  end

  def test_includes_producer_id
    assert_includes logged_data[0], @producer.id
  end

  def test_includes_info_level
    assert_includes logged_data[0], "INFO"
  end

  def test_includes_reloaded_message
    assert_includes logged_data[0], "Producer successfully reloaded"
  end
end

class WaterDropInstrumentationLoggerListenerOnErrorOccurredTest <
  WaterDropInstrumentationLoggerListenerTest
  def setup
    super
    @details[:type] = "error.type"
    @listener.on_error_occurred(@event)
  end

  def test_includes_producer_id
    assert_includes logged_data[0], @producer.id
  end

  def test_includes_error_level
    assert_includes logged_data[0], "ERROR"
  end

  def test_includes_error_message
    assert_includes logged_data[0], "Error occurred"
  end
end

class WaterDropInstrumentationLoggerListenerOnTransactionStartedTest <
  WaterDropInstrumentationLoggerListenerTest
  def setup
    super
    @listener.on_transaction_started(@event)
  end

  def test_includes_producer_id
    assert_includes logged_data[0], @producer.id
  end

  def test_includes_info_level
    assert_includes logged_data[0], "INFO"
  end

  def test_includes_starting_transaction_message
    assert_includes logged_data[0], "Starting transaction"
  end
end

class WaterDropInstrumentationLoggerListenerOnTransactionAbortedTest <
  WaterDropInstrumentationLoggerListenerTest
  def setup
    super
    @listener.on_transaction_aborted(@event)
  end

  def test_includes_producer_id
    assert_includes logged_data[0], @producer.id
  end

  def test_includes_info_level
    assert_includes logged_data[0], "INFO"
  end

  def test_includes_aborting_transaction_message
    assert_includes logged_data[0], "Aborting transaction"
  end
end

class WaterDropInstrumentationLoggerListenerOnTransactionCommittedTest <
  WaterDropInstrumentationLoggerListenerTest
  def setup
    super
    @listener.on_transaction_committed(@event)
  end

  def test_includes_producer_id
    assert_includes logged_data[0], @producer.id
  end

  def test_includes_info_level
    assert_includes logged_data[0], "INFO"
  end

  def test_includes_committing_transaction_message
    assert_includes logged_data[0], "Committing transaction"
  end
end

class WaterDropInstrumentationLoggerListenerOnTransactionMarkedAsConsumedTest <
  WaterDropInstrumentationLoggerListenerTest
  def setup
    super
    test_message_struct = Struct.new(:topic, :partition, :offset, keyword_init: true)
    @details[:message] = test_message_struct.new(
      topic: rand.to_s,
      partition: 0,
      offset: 100
    )
    @listener.on_transaction_marked_as_consumed(@event)
  end

  def test_includes_producer_id
    assert_includes logged_data[0], @producer.id
  end

  def test_includes_info_level
    assert_includes logged_data[0], "INFO"
  end

  def test_includes_marking_message
    assert_includes logged_data[0], "Marking message"
  end
end
