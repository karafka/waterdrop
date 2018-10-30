# frozen_string_literal: true

RSpec.describe WaterDrop::Schemas::Config do
  let(:schema) { described_class }
  let(:config) do
    {
      client_id: 'id',
      logger: NullLogger.new,
      deliver: false,
      raise_on_buffer_overflow: true,
      kafka: {
        seed_brokers: %w[kafka://127.0.0.1:9092],
        connect_timeout: 10,
        socket_timeout: 30,
        max_buffer_bytesize: 10,
        max_buffer_size: 10,
        max_queue_size: 10,
        ack_timeout: 5,
        delivery_interval: 5,
        delivery_threshold: 100,
        max_retries: 2,
        required_acks: 1,
        retry_backoff: 1,
        compression_threshold: 1,
        compression_codec: nil
      }.merge(ssl_details)
    }
  end
  let(:ssl_details) do
    {
      ssl_ca_cert: 'ca_cert',
      ssl_client_cert: 'client_cert',
      ssl_client_cert_key: 'client_cert_key',
      ssl_ca_certs_from_system: true,
      ssl_client_cert_chain: nil,
      sasl_over_ssl: true
    }
  end

  context 'when config is valid' do
    it { expect(schema.call(config)).to be_success }
  end

  context 'when we run client_id validations' do
    context 'when client_id is nil but present in options' do
      before { config[:client_id] = nil }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when client_id is not a string' do
      before { config[:client_id] = nil }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when client_id has an invalid format' do
      before { config[:client_id] = '$%^&*(' }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when client_id is not present' do
      before { config.delete(:client_id) }

      it { expect(schema.call(config)).not_to be_success }
    end
  end

  context 'when we run logger validations' do
    context 'when logger is nil but present in options' do
      before { config[:logger] = nil }

      it { expect(schema.call(config)).not_to be_success }
    end
  end

  context 'when we run deliver validations' do
    context 'when deliver is nil but present in options' do
      before { config[:deliver] = nil }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when deliver is not present' do
      before { config.delete(:deliver) }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when deliver is not boolean' do
      before { config[:deliver] = rand }

      it { expect(schema.call(config)).not_to be_success }
    end
  end

  context 'when we run raise_on_buffer_overflow validations' do
    context 'when raise_on_buffer_overflow is nil but present in options' do
      before { config[:raise_on_buffer_overflow] = nil }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when raise_on_buffer_overflow is not present' do
      before { config.delete(:raise_on_buffer_overflow) }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when raise_on_buffer_overflow is not boolean' do
      before { config[:raise_on_buffer_overflow] = rand }

      it { expect(schema.call(config)).not_to be_success }
    end
  end

  context 'when we run kafka.seed_brokers validations' do
    context 'when seed_brokers are missing' do
      before { config[:kafka][:seed_brokers] = nil }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when seed_brokers are empty' do
      before { config[:kafka][:seed_brokers] = [] }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when seed_brokers are not an array' do
      before { config[:kafka][:seed_brokers] = rand }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when some of seed_brokers are in an invalid format' do
      before { config[:kafka][:seed_brokers] = %w[kafka://127.0.0.1:9092 invalid-format] }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when all the seed brokers are with ssl' do
      before { config[:kafka][:seed_brokers] = %w[kafka+ssl://127.0.0.1:9092] }

      it { expect(schema.call(config)).to be_success }
    end

    context 'when all the seed brokers are not uris' do
      before { config[:kafka][:seed_brokers] = %w[#$^&* ^&*()] }

      it { expect(schema.call(config)).to be_failure }
      it { expect { schema.call(config).errors }.not_to raise_error }
    end
  end

  context 'when we run kafka.connect_timeout validations' do
    context 'when connect_timeout is nil' do
      before { config[:kafka][:connect_timeout] = nil }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when connect_timeout is missing' do
      before { config[:kafka].delete(:connect_timeout) }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when connect_timeout is 0' do
      before { config[:kafka][:connect_timeout] = 0 }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when connect_timeout less than 0' do
      before { config[:kafka][:connect_timeout] = (rand + 1) * -1 }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when connect_timeout is not a number' do
      before { config[:kafka][:connect_timeout] = rand.to_s }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when connect_timeout is float' do
      before { config[:kafka][:connect_timeout] = rand + 1 }

      it { expect(schema.call(config)).not_to be_success }
    end
  end

  context 'when we run kafka.socket_timeout validations' do
    context 'when socket_timeout is nil' do
      before { config[:kafka][:socket_timeout] = nil }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when socket_timeout is missing' do
      before { config[:kafka].delete(:socket_timeout) }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when socket_timeout is 0' do
      before { config[:kafka][:socket_timeout] = 0 }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when socket_timeout less than 0' do
      before { config[:kafka][:socket_timeout] = (rand + 1) * -1 }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when socket_timeout is not a number' do
      before { config[:kafka][:socket_timeout] = rand.to_s }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when socket_timeout is float' do
      before { config[:kafka][:socket_timeout] = rand + 1 }

      it { expect(schema.call(config)).not_to be_success }
    end
  end

  context 'when we run kafka.compression_threshold validations' do
    context 'when compression_threshold is nil' do
      before { config[:kafka][:compression_threshold] = nil }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when compression_threshold is missing' do
      before { config[:kafka].delete(:compression_threshold) }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when compression_threshold is 0' do
      before { config[:kafka][:compression_threshold] = 0 }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when compression_threshold less than 1' do
      before { config[:kafka][:compression_threshold] = (rand + 1) * -1 }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when compression_threshold is 1' do
      before { config[:kafka][:compression_threshold] = 1 }

      it { expect(schema.call(config)).to be_success }
    end

    context 'when compression_threshold is not a number' do
      before { config[:kafka][:compression_threshold] = rand.to_s }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when compression_threshold is float' do
      before { config[:kafka][:compression_threshold] = rand + 1 }

      it { expect(schema.call(config)).not_to be_success }
    end
  end

  context 'when we run kafka.compression_codec validations' do
    context 'when compression_codec is nil' do
      before { config[:kafka][:compression_codec] = nil }

      it { expect(schema.call(config)).to be_success }
    end

    context 'when compression_codec is missing' do
      before { config[:kafka].delete(:compression_codec) }

      it { expect(schema.call(config)).to be_success }
    end

    context 'when compression_codec is not snappy nor gzip' do
      before { config[:kafka][:compression_codec] = rand.to_s }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when compression_codec is snappy' do
      before { config[:kafka][:compression_codec] = :snappy }

      it { expect(schema.call(config)).to be_success }
    end

    context 'when compression_codec is gzip' do
      before { config[:kafka][:compression_codec] = :gzip }

      it { expect(schema.call(config)).to be_success }
    end

    context 'when compression_codec is lz4' do
      before { config[:kafka][:compression_codec] = :lz4 }

      it { expect(schema.call(config)).to be_success }
    end
  end

  context 'when we run kafka.max_buffer_bytesize validations' do
    context 'when max_buffer_bytesize is nil' do
      before { config[:kafka][:max_buffer_bytesize] = nil }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when max_buffer_bytesize is missing' do
      before { config[:kafka].delete(:max_buffer_bytesize) }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when max_buffer_bytesize is 0' do
      before { config[:kafka][:max_buffer_bytesize] = 0 }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when max_buffer_bytesize less than 0' do
      before { config[:kafka][:max_buffer_bytesize] = (rand + 1) * -1 }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when max_buffer_bytesize is not a number' do
      before { config[:kafka][:max_buffer_bytesize] = rand.to_s }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when max_buffer_bytesize is float' do
      before { config[:kafka][:max_buffer_bytesize] = rand + 1 }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when max_buffer_bytesize is 1' do
      before { config[:kafka][:max_buffer_bytesize] = 1 }

      it { expect(schema.call(config)).to be_success }
    end

    context 'when max_buffer_bytesize is gt then 1' do
      before { config[:kafka][:max_buffer_bytesize] = rand(2..100) }

      it { expect(schema.call(config)).to be_success }
    end
  end

  context 'when we run kafka.max_buffer_size validations' do
    context 'when max_buffer_size is nil' do
      before { config[:kafka][:max_buffer_size] = nil }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when max_buffer_size is missing' do
      before { config[:kafka].delete(:max_buffer_size) }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when max_buffer_size is 0' do
      before { config[:kafka][:max_buffer_size] = 0 }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when max_buffer_size less than 0' do
      before { config[:kafka][:max_buffer_size] = (rand + 1) * -1 }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when max_buffer_size is not a number' do
      before { config[:kafka][:max_buffer_size] = rand.to_s }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when max_buffer_size is float' do
      before { config[:kafka][:max_buffer_size] = rand + 1 }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when max_buffer_size is 1' do
      before { config[:kafka][:max_buffer_size] = 1 }

      it { expect(schema.call(config)).to be_success }
    end

    context 'when max_buffer_size is gt then 1' do
      before { config[:kafka][:max_buffer_size] = rand(2..100) }

      it { expect(schema.call(config)).to be_success }
    end
  end

  context 'when we run kafka.max_queue_size validations' do
    context 'when max_queue_size is nil' do
      before { config[:kafka][:max_queue_size] = nil }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when max_queue_size is missing' do
      before { config[:kafka].delete(:max_queue_size) }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when max_queue_size is 0' do
      before { config[:kafka][:max_queue_size] = 0 }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when max_queue_size less than 0' do
      before { config[:kafka][:max_queue_size] = (rand + 1) * -1 }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when max_queue_size is not a number' do
      before { config[:kafka][:max_queue_size] = rand.to_s }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when max_queue_size is float' do
      before { config[:kafka][:max_queue_size] = rand + 1 }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when max_queue_size is 1' do
      before { config[:kafka][:max_queue_size] = 1 }

      it { expect(schema.call(config)).to be_success }
    end

    context 'when max_queue_size is gt then 1' do
      before { config[:kafka][:max_queue_size] = rand(2..100) }

      it { expect(schema.call(config)).to be_success }
    end
  end

  context 'when we run kafka.ack_timeout validations' do
    context 'when ack_timeout is nil' do
      before { config[:kafka][:ack_timeout] = nil }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when ack_timeout is missing' do
      before { config[:kafka].delete(:ack_timeout) }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when ack_timeout is 0' do
      before { config[:kafka][:ack_timeout] = 0 }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when ack_timeout less than 0' do
      before { config[:kafka][:ack_timeout] = (rand + 1) * -1 }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when ack_timeout is not a number' do
      before { config[:kafka][:ack_timeout] = rand.to_s }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when ack_timeout is float' do
      before { config[:kafka][:ack_timeout] = rand + 1 }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when ack_timeout is 1' do
      before { config[:kafka][:ack_timeout] = 1 }

      it { expect(schema.call(config)).to be_success }
    end

    context 'when ack_timeout is gt then 1' do
      before { config[:kafka][:ack_timeout] = rand(2..100) }

      it { expect(schema.call(config)).to be_success }
    end
  end

  context 'when we run kafka.delivery_interval validations' do
    context 'when delivery_interval is nil' do
      before { config[:kafka][:delivery_interval] = nil }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when delivery_interval is missing' do
      before { config[:kafka].delete(:delivery_interval) }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when delivery_interval is 0' do
      before { config[:kafka][:delivery_interval] = 0 }

      it { expect(schema.call(config)).to be_success }
    end

    context 'when delivery_interval less than 0' do
      before { config[:kafka][:delivery_interval] = (rand + 1) * -1 }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when delivery_interval is not a number' do
      before { config[:kafka][:delivery_interval] = rand.to_s }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when delivery_interval is float' do
      before { config[:kafka][:delivery_interval] = rand + 1 }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when delivery_interval is 1' do
      before { config[:kafka][:delivery_interval] = 1 }

      it { expect(schema.call(config)).to be_success }
    end

    context 'when delivery_interval is gt then 1' do
      before { config[:kafka][:delivery_interval] = rand(2..100) }

      it { expect(schema.call(config)).to be_success }
    end
  end

  context 'when we run kafka.delivery_threshold validations' do
    context 'when delivery_threshold is nil' do
      before { config[:kafka][:delivery_threshold] = nil }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when delivery_threshold is missing' do
      before { config[:kafka].delete(:delivery_threshold) }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when delivery_threshold is 0' do
      before { config[:kafka][:delivery_threshold] = 0 }

      it { expect(schema.call(config)).to be_success }
    end

    context 'when delivery_threshold less than 0' do
      before { config[:kafka][:delivery_threshold] = (rand + 1) * -1 }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when delivery_threshold is not a number' do
      before { config[:kafka][:delivery_threshold] = rand.to_s }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when delivery_threshold is float' do
      before { config[:kafka][:delivery_threshold] = rand + 1 }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when delivery_threshold is 1' do
      before { config[:kafka][:delivery_threshold] = 1 }

      it { expect(schema.call(config)).to be_success }
    end

    context 'when delivery_threshold is gt then 1' do
      before { config[:kafka][:delivery_threshold] = rand(2..100) }

      it { expect(schema.call(config)).to be_success }
    end
  end

  context 'when we run kafka.max_retries validations' do
    context 'when max_retries is nil' do
      before { config[:kafka][:max_retries] = nil }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when max_retries is missing' do
      before { config[:kafka].delete(:max_retries) }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when max_retries is 0' do
      before { config[:kafka][:max_retries] = 0 }

      it { expect(schema.call(config)).to be_success }
    end

    context 'when max_retries less than 0' do
      before { config[:kafka][:max_retries] = (rand + 1) * -1 }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when max_retries is not a number' do
      before { config[:kafka][:max_retries] = rand.to_s }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when max_retries is float' do
      before { config[:kafka][:max_retries] = rand + 1 }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when max_retries is 1' do
      before { config[:kafka][:max_retries] = 1 }

      it { expect(schema.call(config)).to be_success }
    end

    context 'when max_retries is gt then 1' do
      before { config[:kafka][:max_retries] = rand(2..100) }

      it { expect(schema.call(config)).to be_success }
    end
  end

  context 'when we run kafka.retry_backoff validations' do
    context 'when retry_backoff is nil' do
      before { config[:kafka][:retry_backoff] = nil }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when retry_backoff is missing' do
      before { config[:kafka].delete(:retry_backoff) }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when retry_backoff is 0' do
      before { config[:kafka][:retry_backoff] = 0 }

      it { expect(schema.call(config)).to be_success }
    end

    context 'when retry_backoff less than 0' do
      before { config[:kafka][:retry_backoff] = (rand + 1) * -1 }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when retry_backoff is not a number' do
      before { config[:kafka][:retry_backoff] = rand.to_s }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when retry_backoff is float' do
      before { config[:kafka][:retry_backoff] = rand + 1 }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when retry_backoff is 1' do
      before { config[:kafka][:retry_backoff] = 1 }

      it { expect(schema.call(config)).to be_success }
    end

    context 'when retry_backoff is gt then 1' do
      before { config[:kafka][:retry_backoff] = rand(2..100) }

      it { expect(schema.call(config)).to be_success }
    end
  end

  context 'when we run kafka.required_acks validations' do
    context 'when required_acks is nil' do
      before { config[:kafka][:required_acks] = nil }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when required_acks is missing' do
      before { config[:kafka].delete(:required_acks) }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when required_acks is not a valid value' do
      before { config[:kafka][:required_acks] = rand }

      it { expect(schema.call(config)).not_to be_success }
    end

    [1, 0, -1, :all].each do |allowed_value|
      context "when required_acks is #{allowed_value}" do
        before { config[:kafka][:required_acks] = allowed_value }

        it { expect(schema.call(config)).to be_success }
      end
    end
  end
  %i[
    ssl_ca_cert
    ssl_ca_cert_file_path
    sasl_gssapi_principal
    sasl_gssapi_keytab
    sasl_plain_authzid
    sasl_plain_username
    sasl_plain_password
    sasl_scram_username
    sasl_scram_password
    sasl_scram_mechanism
    ssl_client_cert_chain
  ].each do |encryption_attribute|
    context "when we validate #{encryption_attribute}" do
      context "when #{encryption_attribute} is nil" do
        before { config[:kafka][encryption_attribute] = nil }

        it { expect(schema.call(config)).to be_success }
      end

      context "when #{encryption_attribute} is not a string" do
        before { config[:kafka][encryption_attribute] = 2 }

        it { expect(schema.call(config)).not_to be_success }
      end
    end
  end

  context 'when we validate ssl_client_cert' do
    context 'when ssl_client_cert is nil and ssl_client_cert_key is nil' do
      before do
        config[:kafka][:ssl_client_cert] = nil
        config[:kafka][:ssl_client_cert_key] = nil
      end

      it { expect(schema.call(config)).to be_success }
    end

    context 'when ssl_client_cert is nil and ssl_client_cert_key is not nil' do
      before { config[:kafka][:ssl_client_cert] = nil }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when ssl_client_cert is not a string' do
      before { config[:kafka][:ssl_client_cert] = 2 }

      it { expect(schema.call(config)).not_to be_success }
    end
  end

  context 'when we validate ssl_client_cert_key' do
    context 'when ssl_client_cert_key is nil and ssl_client_cert is nil' do
      before do
        config[:kafka][:ssl_client_cert_key] = nil
        config[:kafka][:ssl_client_cert] = nil
      end

      it { expect(schema.call(config)).to be_success }
    end

    context 'when ssl_client_cert_key is nil and ssl_client_cert is not nil' do
      before { config[:kafka][:ssl_client_cert_key] = nil }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when ssl_client_cert_key is not a string' do
      before { config[:kafka][:ssl_client_cert_key] = 2 }

      it { expect(schema.call(config)).not_to be_success }
    end
  end

  context 'when we validate ssl_client_cert_chain' do
    context 'when ssl_client_cert_chain is nil and ssl_client_cert is nil' do
      before do
        config[:kafka][:ssl_client_cert_chain] = nil
        config[:kafka][:ssl_client_cert] = nil
        config[:kafka][:ssl_client_cert_key] = nil
      end

      it { expect(schema.call(config)).to be_success }
    end

    context 'when ssl_client_cert_chain is present but ssl_client_cert is nil' do
      before do
        config[:kafka][:ssl_client_cert_chain] = 'chain'
        config[:kafka][:ssl_client_cert] = nil
      end

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when ssl_client_cert_chain is present but ssl_client_cert_key is nil' do
      before do
        config[:kafka][:ssl_client_cert_chain] = 'chain'
        config[:kafka][:ssl_client_cert_key] = nil
      end

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when ssl_client_cert_chain is not a string' do
      before { config[:kafka][:ssl_client_cert_chain] = 2 }

      it { expect(schema.call(config)).not_to be_success }
    end
  end

  context 'when we validate ssl_ca_certs_from_system' do
    context 'when ssl_ca_certs_from_system is not a bool' do
      before { config[:kafka][:ssl_ca_certs_from_system] = 2 }

      it { expect(schema.call(config)).not_to be_success }
    end
  end

  context 'when we validate sasl_over_ssl' do
    context 'when sasl_over_ssl is not a bool' do
      before { config[:kafka][:sasl_over_ssl] = 2 }

      it { expect(schema.call(config)).not_to be_success }
    end
  end

  context 'when we validate sasl_scram_mechanism' do
    context 'when sasl_scram_mechanism is nil' do
      before { config[:kafka][:sasl_scram_mechanism] = nil }

      it { expect(schema.call(config)).to be_success }
    end

    context 'when sasl_scram_mechanism is not a string' do
      before { config[:kafka][:sasl_scram_mechanism] = 2 }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when sasl_scram_mechanism is an invalid string' do
      before { config[:kafka][:sasl_scram_mechanism] = rand.to_s }

      it { expect(schema.call(config)).not_to be_success }
    end

    context 'when sasl_scram_mechanism is sha256' do
      before { config[:kafka][:sasl_scram_mechanism] = 'sha256' }

      it { expect(schema.call(config)).to be_success }
    end

    context 'when sasl_scram_mechanism is sha512' do
      before { config[:kafka][:sasl_scram_mechanism] = 'sha512' }

      it { expect(schema.call(config)).to be_success }
    end
  end
end
