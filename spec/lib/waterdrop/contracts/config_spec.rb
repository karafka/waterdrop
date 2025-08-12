# frozen_string_literal: true

RSpec.describe_current do
  subject(:contract_result) { described_class.new.call(config) }

  let(:contract_errors) { contract_result.errors.to_h }
  let(:config) do
    {
      id: SecureRandom.uuid,
      logger: Logger.new('/dev/null'),
      monitor: WaterDrop::Instrumentation::Monitor.new,
      deliver: false,
      client_class: WaterDrop::Clients::Rdkafka,
      max_payload_size: 1024 * 1024,
      max_wait_timeout: 1,
      wait_on_queue_full: true,
      wait_backoff_on_queue_full: 1,
      wait_timeout_on_queue_full: 10,
      wait_backoff_on_transaction_command: 15,
      max_attempts_on_transaction_command_format: 5,
      instrument_on_wait_queue_full: true,
      max_attempts_on_transaction_command: 1,
      idle_disconnect_timeout: 0,
      reload_on_transaction_fatal_error: true,
      oauth: {
        token_provider_listener: false
      },
      kafka: {
        'bootstrap.servers': 'localhost:9092,localhots:9092'
      }
    }
  end

  context 'when config is valid' do
    it { expect(contract_result).to be_success }
  end

  context 'when id is missing' do
    before { config.delete(:id) }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:id]).not_to be_empty }
  end

  context 'when id is nil' do
    before { config[:id] = nil }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:id]).not_to be_empty }
  end

  context 'when id is not a string' do
    before { config[:id] = rand }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:id]).not_to be_empty }
  end

  context 'when monitor is missing' do
    before { config.delete(:monitor) }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:monitor]).not_to be_empty }
  end

  context 'when monitor is nil' do
    before { config[:monitor] = nil }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:monitor]).not_to be_empty }
  end

  context 'when client_class is missing' do
    before { config.delete(:client_class) }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:client_class]).not_to be_empty }
  end

  context 'when client_class is nil' do
    before { config[:client_class] = nil }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:client_class]).not_to be_empty }
  end

  context 'when logger is missing' do
    before { config.delete(:logger) }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:logger]).not_to be_empty }
  end

  context 'when logger is nil' do
    before { config[:logger] = nil }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:logger]).not_to be_empty }
  end

  context 'when deliver is missing' do
    before { config.delete(:deliver) }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:deliver]).not_to be_empty }
  end

  context 'when deliver is nil' do
    before { config[:deliver] = nil }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:deliver]).not_to be_empty }
  end

  context 'when deliver is not a string' do
    before { config[:deliver] = rand }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:deliver]).not_to be_empty }
  end

  context 'when kafka is missing' do
    before { config.delete(:kafka) }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:kafka]).not_to be_empty }
  end

  context 'when kafka is an empty hash' do
    before { config[:kafka] = {} }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:kafka]).not_to be_empty }
  end

  context 'when kafka hash is present' do
    context 'when there is a non-symbol key setting' do
      before { config[:kafka] = { 'not_a_symbol' => true } }

      it { expect(contract_result).not_to be_success }
    end
  end

  context 'when max_payload_size is nil' do
    before { config[:max_payload_size] = nil }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:max_payload_size]).not_to be_empty }
  end

  context 'when max_payload_size is a negative int' do
    before { config[:max_payload_size] = -1 }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:max_payload_size]).not_to be_empty }
  end

  context 'when max_payload_size is a negative float' do
    before { config[:max_payload_size] = -0.1 }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:max_payload_size]).not_to be_empty }
  end

  context 'when max_payload_size is 0' do
    before { config[:max_payload_size] = 0 }

    it { expect(contract_result).not_to be_success }
  end

  context 'when max_payload_size is positive int' do
    before { config[:max_payload_size] = 1 }

    it { expect(contract_result).to be_success }
  end

  context 'when max_payload_size is positive float' do
    before { config[:max_payload_size] = 1.1 }

    it { expect(contract_result).not_to be_success }
  end

  context 'when max_attempts_on_transaction_command is nil' do
    before { config[:max_attempts_on_transaction_command] = nil }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:max_attempts_on_transaction_command]).not_to be_empty }
  end

  context 'when max_attempts_on_transaction_command is a negative int' do
    before { config[:max_attempts_on_transaction_command] = -1 }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:max_attempts_on_transaction_command]).not_to be_empty }
  end

  context 'when max_attempts_on_transaction_command is a negative float' do
    before { config[:max_attempts_on_transaction_command] = -0.1 }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:max_attempts_on_transaction_command]).not_to be_empty }
  end

  context 'when max_attempts_on_transaction_command is 0' do
    before { config[:max_attempts_on_transaction_command] = 0 }

    it { expect(contract_result).not_to be_success }
  end

  context 'when max_attempts_on_transaction_command is positive int' do
    before { config[:max_attempts_on_transaction_command] = 1 }

    it { expect(contract_result).to be_success }
  end

  context 'when max_attempts_on_transaction_command is positive float' do
    before { config[:max_attempts_on_transaction_command] = 1.1 }

    it { expect(contract_result).not_to be_success }
  end

  context 'when max_wait_timeout is missing' do
    before { config.delete(:max_wait_timeout) }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:max_wait_timeout]).not_to be_empty }
  end

  context 'when max_wait_timeout is nil' do
    before { config[:max_wait_timeout] = nil }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:max_wait_timeout]).not_to be_empty }
  end

  context 'when max_wait_timeout is a negative int' do
    before { config[:max_wait_timeout] = -1 }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:max_wait_timeout]).not_to be_empty }
  end

  context 'when max_wait_timeout is a negative float' do
    before { config[:max_wait_timeout] = -0.1 }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:max_wait_timeout]).not_to be_empty }
  end

  context 'when max_wait_timeout is 0' do
    before { config[:max_wait_timeout] = 0 }

    it { expect(contract_result).to be_success }
  end

  context 'when max_wait_timeout is positive int' do
    before { config[:max_wait_timeout] = 1 }

    it { expect(contract_result).to be_success }
  end

  context 'when max_wait_timeout is positive float' do
    before { config[:max_wait_timeout] = 1.1 }

    it { expect(contract_result).to be_success }
  end

  context 'when wait_on_queue_full is not a boolean' do
    before { config[:wait_on_queue_full] = 0 }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:wait_on_queue_full]).not_to be_empty }
  end

  context 'when reload_on_transaction_fatal_error is not a boolean' do
    before { config[:reload_on_transaction_fatal_error] = 0 }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:reload_on_transaction_fatal_error]).not_to be_empty }
  end

  context 'when instrument_on_wait_queue_full is not a boolean' do
    before { config[:instrument_on_wait_queue_full] = 0 }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:instrument_on_wait_queue_full]).not_to be_empty }
  end

  context 'when wait_backoff_on_queue_full is not a numeric' do
    before { config[:wait_backoff_on_queue_full] = 'na' }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:wait_backoff_on_queue_full]).not_to be_empty }
  end

  context 'when wait_backoff_on_queue_full is less than 0' do
    before { config[:wait_backoff_on_queue_full] = -1 }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:wait_backoff_on_queue_full]).not_to be_empty }
  end

  context 'when wait_backoff_on_transaction_command is not a numeric' do
    before { config[:wait_backoff_on_transaction_command] = 'na' }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:wait_backoff_on_transaction_command]).not_to be_empty }
  end

  context 'when wait_backoff_on_transaction_command is less than 0' do
    before { config[:wait_backoff_on_transaction_command] = -1 }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:wait_backoff_on_transaction_command]).not_to be_empty }
  end

  context 'when wait_timeout_on_queue_full is not a numeric' do
    before { config[:wait_timeout_on_queue_full] = 'na' }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:wait_timeout_on_queue_full]).not_to be_empty }
  end

  context 'when wait_timeout_on_queue_full is less than 0' do
    before { config[:wait_timeout_on_queue_full] = -1 }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:wait_timeout_on_queue_full]).not_to be_empty }
  end

  context 'when oauth token_provider_listener does not respond to on_oauthbearer_token_refresh' do
    before { config[:oauth][:token_provider_listener] = true }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:'oauth.token_provider_listener']).not_to be_empty }
  end

  context 'when oauth token_provider_listener responds to on_oauthbearer_token_refresh' do
    let(:listener) do
      Class.new do
        def on_oauthbearer_token_refresh(_); end
      end
    end

    before { config[:oauth][:token_provider_listener] = listener.new }

    it { expect(contract_result).to be_success }
  end

  context 'when idle_disconnect_timeout is missing' do
    before { config.delete(:idle_disconnect_timeout) }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:idle_disconnect_timeout]).not_to be_empty }
  end

  context 'when idle_disconnect_timeout is nil' do
    before { config[:idle_disconnect_timeout] = nil }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:idle_disconnect_timeout]).not_to be_empty }
  end

  context 'when idle_disconnect_timeout is not an integer' do
    before { config[:idle_disconnect_timeout] = 30.5 }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:idle_disconnect_timeout]).not_to be_empty }
  end

  context 'when idle_disconnect_timeout is a negative integer' do
    before { config[:idle_disconnect_timeout] = -1000 }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:idle_disconnect_timeout]).not_to be_empty }
  end

  context 'when idle_disconnect_timeout is below minimum (30 seconds)' do
    before { config[:idle_disconnect_timeout] = 29_999 }

    it { expect(contract_result).not_to be_success }
    it { expect(contract_errors[:idle_disconnect_timeout]).not_to be_empty }
  end

  context 'when idle_disconnect_timeout is zero (disabled)' do
    before { config[:idle_disconnect_timeout] = 0 }

    it { expect(contract_result).to be_success }
  end

  context 'when idle_disconnect_timeout is exactly minimum (30 seconds)' do
    before { config[:idle_disconnect_timeout] = 30_000 }

    it { expect(contract_result).to be_success }
  end

  context 'when idle_disconnect_timeout is above minimum' do
    before { config[:idle_disconnect_timeout] = 60_000 }

    it { expect(contract_result).to be_success }
  end
end
