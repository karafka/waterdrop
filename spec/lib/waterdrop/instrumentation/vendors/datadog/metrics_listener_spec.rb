# frozen_string_literal: true

RSpec.describe_current do
  subject(:listener) do
    client = described_class.new

    client.setup do |config|
      config.client = dummy_client
    end

    client
  end

  # We do not need DD to test this integration, we can use dummy client with the same API
  # It has all the methods that we report with and flushes results to an internal accumulator
  # that we can use to compare results with
  let(:dummy_client) do
    Class.new do
      attr_reader :buffer

      def initialize
        @buffer = Hash.new do |buffer, dd_method|
          buffer[dd_method] = Hash.new do |key_scope, metric|
            key_scope[metric] = []
          end
        end
      end

      %i[
        count
        histogram
        gauge
        increment
        decrement
      ].each do |method_name|
        define_method method_name do |metric, value = nil, details = {}|
          @buffer[method_name][metric] << [value, details]
        end
      end
    end.new
  end

  let(:producer) do
    producer = create(:producer)
    producer.monitor.subscribe listener
    producer
  end

  after { producer.close }

  context 'when having some default tags present' do
    subject(:listener) do
      client = described_class.new

      client.setup do |config|
        config.client = dummy_client
        config.default_tags = %w[test:me]
      end

      client
    end

    context 'when publishing, default tags should be included' do
      before { producer.produce_sync(topic: rand.to_s, payload: rand.to_s) }

      it 'expect to have proper metrics data in place' do
        published_tags = dummy_client.buffer[:increment]['waterdrop.produced_sync'][0][0][:tags]
        expect(published_tags).to include('test:me')
      end
    end
  end

  context 'when setting up a listener upon initialization' do
    let(:listener) do
      described_class.new do |config|
        config.client = dummy_client
      end
    end

    it 'expect to work' do
      expect(listener.client).to eq(dummy_client)
    end
  end

  # Here we focus on metrics coming from rdkafka, we dispatch this single message just to have
  # some fluctuation
  context 'when expecting emitted stats DD dispatch' do
    # Just to trigger some stats
    before do
      producer.produce_sync(topic: rand.to_s, payload: rand.to_s)

      # Give it some time to emit the stats
      sleep(1)
    end

    let(:counts) { dummy_client.buffer[:count] }
    let(:histograms) { dummy_client.buffer[:histogram] }
    let(:guages) { dummy_client.buffer[:gauge] }
    let(:broker_tag) { { tags: %w[broker:localhost:9092] } }

    # We add all expectations in one example not to sleep each time
    it 'expect to have proper metrics in place' do
      # count
      expect(counts['waterdrop.calls']).to include([0, { tags: [] }])
      expect(counts['waterdrop.calls'].uniq.size).to be > 1
      expect(counts['waterdrop.deliver.attempts']).to include([0, broker_tag])
      expect(counts['waterdrop.deliver.errors']).to include([0, broker_tag])
      expect(counts['waterdrop.receive.errors']).to include([0, broker_tag])

      # histogram
      expect(histograms['waterdrop.queue.size']).to include([0, { tags: [] }])
      # -1 here means, one message was removed from the queue as we use histogram for tracking
      expect(histograms['waterdrop.queue.size']).to include([-1, { tags: [] }])

      # gauge
      expect(guages['waterdrop.queue.latency.avg'].uniq.size).to be > 1
      expect(guages['waterdrop.queue.latency.avg']).to include([0, broker_tag])
      expect(guages['waterdrop.queue.latency.p95'].uniq.size).to be > 1
      expect(guages['waterdrop.queue.latency.p95']).to include([0, broker_tag])
      expect(guages['waterdrop.queue.latency.p99'].uniq.size).to be > 1
      expect(guages['waterdrop.queue.latency.p99']).to include([0, broker_tag])
      expect(guages['waterdrop.network.latency.avg'].uniq.size).to be > 1
      expect(guages['waterdrop.network.latency.avg']).to include([0, broker_tag])
      expect(guages['waterdrop.network.latency.p95'].uniq.size).to be > 1
      expect(guages['waterdrop.network.latency.p95']).to include([0, broker_tag])
      expect(guages['waterdrop.network.latency.p99'].uniq.size).to be > 1
      expect(guages['waterdrop.network.latency.p99']).to include([0, broker_tag])
    end
  end

  context 'when producing sync' do
    before do
      producer.produce_sync(topic: rand.to_s, payload: rand.to_s)

      producer.produce_many_sync(
        [
          { topic: rand.to_s, payload: rand.to_s },
          { topic: rand.to_s, payload: rand.to_s }
        ]
      )
    end

    it 'expect to have proper metrics data in place' do
      expect(dummy_client.buffer[:increment]['waterdrop.produced_sync'].size).to eq(3)
    end
  end

  context 'when producing async' do
    before do
      producer.produce_async(topic: rand.to_s, payload: rand.to_s)

      producer.produce_many_async(
        [
          { topic: rand.to_s, payload: rand.to_s },
          { topic: rand.to_s, payload: rand.to_s }
        ]
      )
    end

    it 'expect to have proper metrics data in place' do
      expect(dummy_client.buffer[:increment]['waterdrop.produced_async'].size).to eq(3)
    end
  end

  context 'when buffering' do
    before do
      producer.buffer(topic: rand.to_s, payload: rand.to_s)

      producer.buffer_many(
        [
          { topic: rand.to_s, payload: rand.to_s },
          { topic: rand.to_s, payload: rand.to_s }
        ]
      )
    end

    it 'expect to have proper metrics data in place' do
      expect(dummy_client.buffer[:histogram]['waterdrop.buffer.size'].size).to eq(2)
    end
  end

  context 'when flushing sync' do
    before do
      producer.buffer(topic: rand.to_s, payload: rand.to_s)
      producer.flush_sync
    end

    it 'expect to have proper metrics data in place' do
      expect(dummy_client.buffer[:increment]['waterdrop.flushed_sync'].size).to eq(1)
    end
  end

  context 'when flushing async' do
    before do
      producer.buffer(topic: rand.to_s, payload: rand.to_s)
      producer.flush_async
    end

    it 'expect to have proper metrics data in place' do
      expect(dummy_client.buffer[:increment]['waterdrop.flushed_async'].size).to eq(1)
    end
  end

  context 'when message is acknowledged' do
    before do
      producer.produce_sync(topic: rand.to_s, payload: rand.to_s)
      # We need to give the async callback a bit of time to kick in
      sleep(0.1)
    end

    it 'expect to have a proper metric in place' do
      expect(dummy_client.buffer[:increment]['waterdrop.acknowledged'].size).to eq(1)
    end
  end

  context 'when error occurred' do
    let(:producer) do
      producer = create(
        :producer,
        kafka: {
          'bootstrap.servers': 'localhost:9093',
          'statistics.interval.ms': 100,
          'message.timeout.ms': 100
        }
      )
      producer.monitor.subscribe listener
      producer
    end

    before do
      producer.produce_async(topic: rand.to_s, payload: rand.to_s)
      sleep(0.1)
    end

    it 'expect error count to increase' do
      expect(dummy_client.buffer[:count]['waterdrop.error_occurred']).not_to be_empty
    end
  end

  context 'when we encounter a node with id -1' do
    let(:metric) { described_class.new.rd_kafka_metrics.last }

    let(:statistics) do
      {
        'brokers' => {
          'node_name' => { 'nodeid' => -1 }
        }
      }
    end

    it 'expect not to publish metrics on it' do
      expect { listener.send(:report_metric, metric, statistics) }.not_to raise_error
    end
  end

  context 'when we try to publish a non-existing metric' do
    let(:metric) { described_class::RdKafkaMetric.new(:count, :na, 'na') }
    let(:statistics) { {} }

    it do
      expect { listener.send(:report_metric, metric, statistics) }.to raise_error(ArgumentError)
    end
  end
end
