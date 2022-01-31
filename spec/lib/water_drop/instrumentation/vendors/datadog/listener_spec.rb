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
      ].each do |method_name|
        define_method method_name do |metric, value, details = {}|
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

  # Here we focus on metrics coming from rdkafka, we dispatch this single message just to have
  # some fluctuation
  context 'when expecting emitted stats DD dispatch' do
    # Just to trigger some stats
    before do
      producer.produce_sync(topic: rand.to_s, payload: rand.to_s)

      # Give it some time to emit the stats
      sleep(0.2)
    end

    let(:counts) { dummy_client.buffer[:count] }
    let(:histograms) { dummy_client.buffer[:histogram] }
    let(:guages) { dummy_client.buffer[:gauge] }
    let(:broker_tag) { { tags: %w[broker:localhost:9092] } }

    it 'expect to have proper metrics data in place' do
      # count
      expect(counts['waterdrop.calls']).to include([0, {}])
      expect(counts['waterdrop.calls']).to include([1, {}])
      expect(counts['waterdrop.deliver.attempts']).to include([0, broker_tag])
      expect(counts['waterdrop.deliver.errors']).to include([0, broker_tag])
      expect(counts['waterdrop.receive.errors']).to include([0, broker_tag])

      # histogram
      expect(histograms['waterdrop.queue.size']).to include([0, {}])
      # -1 here means, one message was removed from the queue as we use histogram for tracking
      expect(histograms['waterdrop.queue.size']).to include([-1, {}])

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
      expect(dummy_client.buffer[:increment]['waterdrop.producer.produced_sync'].size).to eq(3)
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
      expect(dummy_client.buffer[:increment]['waterdrop.producer.produced_async'].size).to eq(3)
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
      expect(dummy_client.buffer[:increment]['waterdrop.producer.buffered'].size).to eq(3)
    end
  end

  context 'when flushing sync' do
    before do
      producer.buffer(topic: rand.to_s, payload: rand.to_s)
      producer.flush_sync
    end

    it 'expect to have proper metrics data in place' do
      expect(dummy_client.buffer[:increment]['waterdrop.producer.flushed_sync'].size).to eq(1)
    end
  end

  context 'when flushing async' do
    before do
      producer.buffer(topic: rand.to_s, payload: rand.to_s)
      producer.flush_async
    end

    it 'expect to have proper metrics data in place' do
      expect(dummy_client.buffer[:increment]['waterdrop.producer.flushed_async'].size).to eq(1)
    end
  end
end
