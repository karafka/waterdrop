# frozen_string_literal: true

RSpec.describe_current do
  subject(:pipe) { described_class.new(client) }

  let(:client) do
    instance_double(Rdkafka::Producer, enable_queue_io_events: nil)
  end

  after do
    pipe.close
  rescue
    # Ignore if pipe was never created
  end

  describe "#initialize" do
    it "creates a readable IO" do
      expect(pipe.reader).to be_a(IO)
    end

    it "calls enable_queue_io_events on the client" do
      allow(client).to receive(:enable_queue_io_events)
      described_class.new(client)
      expect(client).to have_received(:enable_queue_io_events).with(kind_of(Integer))
    end

    context "when enable_queue_io_events raises" do
      let(:client) do
        instance_double(Rdkafka::Producer).tap do |c|
          allow(c).to receive(:enable_queue_io_events).and_raise(StandardError, "test error")
        end
      end

      it "propagates the error" do
        expect { described_class.new(client) }.to raise_error(StandardError, "test error")
      end
    end
  end

  describe "#signal" do
    it "makes the pipe readable" do
      pipe.signal
      ready = IO.select([pipe.reader], nil, nil, 0)
      expect(ready).not_to be_nil
    end

    it "does not raise when called multiple times" do
      expect do
        3.times { pipe.signal }
      end.not_to raise_error
    end

    it "does not raise after pipe is closed" do
      pipe.close
      expect { pipe.signal }.not_to raise_error
    end
  end

  describe "#drain" do
    it "does not raise when pipe is empty" do
      expect { pipe.drain }.not_to raise_error
    end

    it "clears the pipe after signals" do
      3.times { pipe.signal }
      pipe.drain
      ready = IO.select([pipe.reader], nil, nil, 0)
      expect(ready).to be_nil
    end

    it "does not raise after pipe is closed" do
      pipe.close
      expect { pipe.drain }.not_to raise_error
    end
  end

  describe "#close" do
    it "closes the reader IO" do
      reader = pipe.reader
      pipe.close
      expect(reader.closed?).to be(true)
    end

    it "can be called multiple times safely" do
      expect do
        3.times { pipe.close }
      end.not_to raise_error
    end
  end

  describe "#reader" do
    it "can be used with IO.select" do
      # We can't easily trigger librdkafka to write to the pipe in a unit test,
      # but we can verify the reader is a valid IO for select
      expect(pipe.reader).to respond_to(:read_nonblock)
    end
  end
end
