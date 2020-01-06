# frozen_string_literal: true

RSpec.describe WaterDrop::ConfigApplier do
  subject(:sync) { described_class.call(delivery_boy_config, settings) }

  let!(:original_delivery_boy_config) { DeliveryBoy.config }
  let(:delivery_boy_config) { DeliveryBoy.config }

  describe '#call' do
    describe 'when we handle standard, valid cases' do
      before { sync }

      context 'when we sync client_id' do
        let(:settings) { { client_id: rand.to_s } }

        it { expect(delivery_boy_config.client_id).to eq settings[:client_id] }
      end

      context 'when we have valid sasl_scram_mechanism string' do
        let(:settings) { { sasl_scram_mechanism: 'sha512' } }

        it { expect(delivery_boy_config.sasl_scram_mechanism).to eq settings.values.first }
      end

      # Typical int cases
      %i[
        connect_timeout
        socket_timeout
        max_buffer_bytesize
        max_buffer_size
        max_queue_size
        ack_timeout
        delivery_interval
        delivery_threshold
        transactional_timeout
        max_retries
        retry_backoff
      ].each do |key|
        context "when we sync #{key} with int value" do
          let(:settings) { { key => rand(1000..10_000) } }

          it { expect(delivery_boy_config.public_send(key)).to eq settings[key] }
        end
      end

      # Typical bool cases
      %i[
        idempotent
        transactional
        ssl_ca_certs_from_system
        ssl_verify_hostname
        sasl_over_ssl
      ].each do |key|
        context "when we sync #{key} with the opposite bool value" do
          let(:settings) { { key => !original_delivery_boy_config.public_send(key) } }

          it { expect(delivery_boy_config.public_send(key)).to eq settings[key] }
        end
      end
    end

    describe 'when we handle ignored internal settings' do
      context 'when we sync logger' do
        let(:settings) { { logger: [rand.to_s] } }

        it { expect(delivery_boy_config.respond_to?(:logger)).to eq false }
      end

      context 'when we sync deliver' do
        let(:settings) { { deliver: [rand.to_s] } }

        it { expect(delivery_boy_config.respond_to?(:deliver)).to eq false }
      end

      context 'when we sync raise_on_buffer_overflow' do
        let(:settings) { { raise_on_buffer_overflow: [rand.to_s] } }

        it { expect(delivery_boy_config.respond_to?(:raise_on_buffer_overflow)).to eq false }
      end
    end

    describe 'when we handle special handling cases' do
      before { sync }

      context 'when we sync seed_brokers' do
        let(:settings) { { seed_brokers: [rand.to_s] } }

        it { expect(delivery_boy_config.brokers).to eq settings[:seed_brokers] }
      end
    end
  end
end
