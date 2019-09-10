# WaterDrop

[![Build Status](https://travis-ci.org/karafka/waterdrop.svg)](https://travis-ci.org/karafka/waterdrop)
[![Join the chat at https://gitter.im/karafka/karafka](https://badges.gitter.im/karafka/karafka.svg)](https://gitter.im/karafka/karafka?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Gem used to send messages to Kafka in an easy way with an extra validation layer. It is a part of the [Karafka](https://github.com/karafka/karafka) ecosystem.

It:

 - Is thread safe
 - Supports sync producing
 - Supports async producing
 - Supports buffering
 - Supports producing messages to multiple clusters
 - Supports multiple delivery policies
 - Works with Kafka 1.0+ and Ruby 2.4+

## Installation

```ruby
gem install waterdrop
```

or add this to your Gemfile:

```ruby
gem 'waterdrop'
```

and run

```
bundle install
```

## Setup

WaterDrop is a complex tool, that contains multiple configuration options. To keep everything organized, all the configuration options were divided into two groups:

- WaterDrop options - options directly related to Karafka framework and it's components
- Kafka driver options - options related to `Kafka`

To apply all those configuration options, you need to create a producer instance and use the ```#setup``` method:

```ruby
producer = WaterDrop::Producer.new

producer.setup do |config|
  config.deliver = true
  config.kafka = {
    'bootstrap.servers' => 'localhost:9092',
    'request.required.acks' => 1
  }
end
```

or you can do the same while initializing the producer:

```ruby
producer = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.kafka = {
    'bootstrap.servers' => 'localhost:9092',
    'request.required.acks' => 1
  }
end
```

### WaterDrop configuration options

| Option             | Description                                                     |
|--------------------|-----------------------------------------------------------------|
| `id`               | id of the producer for instrumentation and logging              |
| `logger`           | Logger that we want to use                                      |
| `deliver`          | Should we send messages to Kafka or just fake the delivery      |
| `max_wait_timeout` | Waits that long for the delivery report or raises an error      |
| `wait_timeout`     | Waits that long before re-check of delivery report availability |

### Kafka configuration options

You can create producers with different `kafka` settings. Documentation of the available configuration options is available on https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md.

## Usage

Please refer to the [documentation](https://www.rubydoc.info/github/karafka/waterdrop) in case you're interested in the more advanced API.

### Basic usage

To send Kafka messages, just create a producer and use it:

```ruby
producer = WaterDrop::Producer.new

producer.setup do |config|
  config.kafka = { 'bootstrap.servers' => 'localhost:9092' }
end

producer.produce_sync(topic: 'my-topic', payload: 'my message')

# or for async
producer.produce_async(topic: 'my-topic', payload: 'my message')

# or in batches
producer.produce_many_sync(
  [
    { topic: 'my-topic', payload: 'my message'},
    { topic: 'my-topic', payload: 'my message'}
  ]
)

# both sync and async
producer.produce_many_async(
  [
    { topic: 'my-topic', payload: 'my message'},
    { topic: 'my-topic', payload: 'my message'}
  ]
)

# Don't forget to close the producer once you're done to flush the internal buffers, etc
producer.close
```

Each message that you want to publish, will have its value checked.

Here are all the things you can provide in the message hash:

| Option      | Required | Value type    | Description                                           |
|-------------|----------|---------------|-------------------------------------------------------|
| `topic`     | true     | String        | The Kafka topic that should be written to             |
| `payload`   | true     | String        | Data you want to send to Kafka                        |
| `key`       | false    | String        | The key that should be set in the Kafka message       |
| `partition` | false    | Integer       | A specific partition number that should be written to |
| `timestamp` | false    | Time, Integer | The timestamp that should be set on the message       |
| `headers`   | false    | Hash          | Headers for the message                               |

Keep in mind, that message you want to send should be either binary or stringified (to_s, to_json, etc).

### Buffering

WaterDrop producers support buffering of messages, which means that you can easily implement periodic flushing for long runinng processes as well as buffer several messages to be flushed the same moment:

```ruby
producer = WaterDrop::Producer.new

producer.setup do |config|
  config.kafka = { 'bootstrap.servers' => 'localhost:9092' }
end

time = Time.now - 10

while time < Time.now
  time += 1
  producer.buffer(topic: 'times', payload: Time.now.to_s)
end

puts "The messages buffer size #{producer.messages.size}"
producer.flush_sync
puts "The messages buffer size #{producer.message.size}"

producer.close
```

## Instrumentation

Each of the producers after the `#setup` is done, has a custom monitor to which you can subscribe.

```ruby
producer = WaterDrop::Producer.new

producer.setup do |config|
  config.kafka = { 'bootstrap.servers' => 'localhost:9092' }
end

producer.monitor.subscribe('message.produced_async') do |event|
  puts "A message was produced to '#{event[:message][:topic]}' topic!"
end

producer.produce_async(topic: 'events', payload: 'data')
```

See the `WaterDrop::Instrumentation::Monitor::EVENTS` for the list of all the supported events.

## References

* [WaterDrop code examples](https://travis-ci.org/karafka/waterdrop/master/examples)
* [WaterDrop code documentation](https://www.rubydoc.info/github/karafka/waterdrop)
* [Karafka framework](https://github.com/karafka/karafka)
* [WaterDrop Travis CI](https://travis-ci.org/karafka/waterdrop)
* [WaterDrop Coditsu](https://app.coditsu.io/karafka/repositories/waterdrop)

## Note on contributions

First, thank you for considering contributing to WaterDrop! It's people like you that make the open source community such a great community!

Each pull request must pass all the RSpec specs and meet our quality requirements.

To check if everything is as it should be, we use [Coditsu](https://coditsu.io) that combines multiple linters and code analyzers for both code and documentation. Once you're done with your changes, submit a pull request.

Coditsu will automatically check your work against our quality standards. You can find your commit check results on the [builds page](https://app.coditsu.io/karafka/repositories/waterdrop/builds/commit_builds) of WaterDrop repository.

[![coditsu](https://coditsu.io/assets/quality_bar.svg)](https://app.coditsu.io/karafka/repositories/waterdrop/builds/commit_builds)
