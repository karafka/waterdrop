# WaterDrop

**Note**: Documentation presented here refers to WaterDrop `2.0.0.pre1`.

WaterDrop `2.0` does **not** work with Karafka `1.*` and aims to either work as a standalone producer outside of Karafka `1.*` ecosystem or as a part of not yet released Karafka `2.0.*`.

Please refer to [this](https://github.com/karafka/waterdrop/tree/1.4) branch and it's documentation for details about WaterDrop `1.*` usage.

[![Build Status](https://github.com/karafka/waterdrop/workflows/ci/badge.svg)](https://github.com/karafka/waterdrop/actions?query=workflow%3Aci)
[![Join the chat at https://gitter.im/karafka/karafka](https://badges.gitter.im/karafka/karafka.svg)](https://gitter.im/karafka/karafka?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Gem used to send messages to Kafka in an easy way with an extra validation layer. It is a part of the [Karafka](https://github.com/karafka/karafka) ecosystem.

It:

 - Is thread safe
 - Supports sync producing
 - Supports async producing
 - Supports buffering
 - Supports producing messages to multiple clusters
 - Supports multiple delivery policies
 - Works with Kafka 1.0+ and Ruby 2.5+

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
    'bootstrap.servers': 'localhost:9092',
    'request.required.acks': 1
  }
end
```

or you can do the same while initializing the producer:

```ruby
producer = WaterDrop::Producer.new do |config|
  config.deliver = true
  config.kafka = {
    'bootstrap.servers': 'localhost:9092',
    'request.required.acks': 1
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
  config.kafka = { 'bootstrap.servers': 'localhost:9092' }
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

WaterDrop producers support buffering of messages, which means that you can easily implement periodic flushing for long running processes as well as buffer several messages to be flushed the same moment:

```ruby
producer = WaterDrop::Producer.new

producer.setup do |config|
  config.kafka = { 'bootstrap.servers': 'localhost:9092' }
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
  config.kafka = { 'bootstrap.servers': 'localhost:9092' }
end

producer.monitor.subscribe('message.produced_async') do |event|
  puts "A message was produced to '#{event[:message][:topic]}' topic!"
end

producer.produce_async(topic: 'events', payload: 'data')

producer.close
```

See the `WaterDrop::Instrumentation::Monitor::EVENTS` for the list of all the supported events.

### Usage statistics

WaterDrop may be configured to emit internal metrics at a fixed interval by setting the `kafka` `statistics.interval.ms` configuration property to a value > `0`. Once that is done, emitted statistics are available after subscribing to the `statistics.emitted` publisher event.

The statistics include all of the metrics from `librdkafka` (full list [here](https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md)) as well as the diff of those against the previously emitted values.

For several attributes like `txmsgs`, `librdkafka` publishes only the totals. In order to make it easier to track the progress (for example number of messages sent between statistics emitted events), WaterDrop diffs all the numeric values against previously available numbers. All of those metrics are available under the same key as the metric but with additional `_d` postfix:


```ruby
producer = WaterDrop::Producer.new do |config|
  config.kafka = {
    'bootstrap.servers': 'localhost:9092',
    'statistics.interval.ms': 2_000 # emit statistics every 2 seconds
  }
end

producer.monitor.subscribe('statistics.emitted') do |event|
  sum = event[:statistics]['txmsgs']
  diff = event[:statistics]['txmsgs_d']

  p "Sent messages: #{sum}"
  p "Messages sent from last statistics report: #{diff}"
end

sleep(2)

# Sent messages: 0
# Messages sent from last statistics report: 0

20.times { producer.produce_async(topic: 'events', payload: 'data') }

# Sent messages: 20
# Messages sent from last statistics report: 20

sleep(2)

20.times { producer.produce_async(topic: 'events', payload: 'data') }

# Sent messages: 40
# Messages sent from last statistics report: 20

sleep(2)

# Sent messages: 40
# Messages sent from last statistics report: 0

producer.close
```

Note: The metrics returned may not be completely consistent between brokers, toppars and totals, due to the internal asynchronous nature of librdkafka. E.g., the top level tx total may be less than the sum of the broker tx values which it represents.

### Forking and potential memory problems

If you work with forked processes, make sure you **don't** use the producer before the fork. You can easily configure the producer and then fork and use it.

To tackle this [obstacle](https://github.com/appsignal/rdkafka-ruby/issues/15) related to rdkafka, WaterDrop adds finalizer to each of the producers to close the rdkafka client before the Ruby process is shutdown. Due to the [nature of the finalizers](https://www.mikeperham.com/2010/02/24/the-trouble-with-ruby-finalizers/), this implementation prevents producers from being GCed (except upon VM shutdown) and can cause memory leaks if you don't use persistent/long-lived producers in a long-running process or if you don't use the `#close` method of a producer when it is no longer needed. Creating a producer instance for each message is anyhow a rather bad idea, so we recommend not to.

## References

* [WaterDrop code documentation](https://www.rubydoc.info/github/karafka/waterdrop)
* [Karafka framework](https://github.com/karafka/karafka)
* [WaterDrop Actions CI](https://github.com/karafka/waterdrop/actions?query=workflow%3Ac)
* [WaterDrop Coditsu](https://app.coditsu.io/karafka/repositories/waterdrop)

## Note on contributions

First, thank you for considering contributing to WaterDrop! It's people like you that make the open source community such a great community!

Each pull request must pass all the RSpec specs and meet our quality requirements.

To check if everything is as it should be, we use [Coditsu](https://coditsu.io) that combines multiple linters and code analyzers for both code and documentation. Once you're done with your changes, submit a pull request.

Coditsu will automatically check your work against our quality standards. You can find your commit check results on the [builds page](https://app.coditsu.io/karafka/repositories/waterdrop/builds/commit_builds) of WaterDrop repository.

[![coditsu](https://coditsu.io/assets/quality_bar.svg)](https://app.coditsu.io/karafka/repositories/waterdrop/builds/commit_builds)
