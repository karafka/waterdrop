# WaterDrop

[![Build Status](https://travis-ci.org/karafka/waterdrop.png)](https://travis-ci.org/karafka/waterdrop)
[![Code Climate](https://codeclimate.com/github/karafka/waterdrop/badges/gpa.svg)](https://codeclimate.com/github/karafka/waterdrop)
[![Gem Version](https://badge.fury.io/rb/waterdrop.svg)](http://badge.fury.io/rb/waterdrop)

Gem used to send messages to Kafka in a standard and in an aspect way.

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

WaterDrop has following configuration options:

| Option                  | Value type    | Description                      |
|-------------------------|---------------|----------------------------------|
| send_messages           | Boolean       | Should we send messages to Kafka |
| kafka_hosts             | Array<String> | Kafka servers hosts with ports   |
| connection_pool_size    | Integer       | Kafka connection pool size       |
| connection_pool_timeout | Integer       | Kafka connection pool timeout    |
| raise_on_failure        | Boolean       | Should we raise an exception when we cannot send message to Kafka - if false will silently ignore failures (will just ignore them) |

To apply this configuration, you need to use a *setup* method:

```ruby
WaterDrop.setup do |config|
  config.send_messages = true
  config.connection_pool_size = 20
  config.connection_pool_timeout = 1
  config.kafka_hosts = ['localhost:9092']
  config.raise_on_failure = true
end
```

This configuration can be placed in *config/initializers* and can vary based on the environment:

```ruby
WaterDrop.setup do |config|
  config.send_messages = Rails.env.production?
  config.connection_pool_size = 20
  config.connection_pool_timeout = 1
  config.kafka_hosts = [Rails.env.production? ? 'prod-host:9091' : 'localhost:9092']
  config.raise_on_failure = Rails.env.production?
end
```

## Usage

### Creating and sending standard messages

To send Kafka messages, you don't need to use aspects, you can create and send messages directly:

```ruby
message = WaterDrop::Message.new('topic', 'message')
message.send!

message = WaterDrop::Message.new('topic', { user_id: 1 }.to_json)
message.send!
```

message that you want to send should be either binary or stringified (to_s, to_json, etc).

### Using aspects to handle messages

WaterDrop uses [Aspector](https://github.com/gcao/aspector) to allow aspect oriented messages hookup. If you need extensive details about aspector usage, please refer to the [examples](https://github.com/gcao/aspector/tree/master/examples) directory of this project.

In general aspects allows adding additional behavior to existing code without modifying the code itself. This way we can create and send messages, without "polluting" the business logic with it.

All the WaterDrop aspects accept following parameters:

| Option                  | Value type            | Description                                    |
|-------------------------|-----------------------|------------------------------------------------|
| ClassName               | Class                 | Class to which we want to hook                 |
| method: :method_name    | Symbol, Array<Symbol> | Method (or methods) to which we want to hook   |
| topic: 'karafka_topic'  | String, Symbol        | Kafka topic to which we will send the message  |

There also a *message*, *after_message* and *before_message* proc parameter that will be evaluated in the methods object context.

#### Before aspects hookup

```ruby
WaterDrop::Aspects::BeforeAspect.apply(
  ClassName,
  method: :run,
  topic: 'karafka_topic',
  message: -> { any_class_name_instance_method }
)
```

now each time before you run:

```ruby
ClassName.new.run
```

a message with the given message will be send to Kafka.

#### After aspects hookup

```ruby
WaterDrop::Aspects::AfterAspect.apply(
  ClassName,
  method: :run,
  topic: 'karafka_topic',
  message: ->(result) { "This is result of method run: #{result}" }
)
```

now each time after you run:

```ruby
ClassName.new.run
```

a message with the given message will be send to Kafka.

#### Around aspects hookup

```ruby
WaterDrop::Aspects::AroundAspect.apply(
  ClassName,
  method: :run,
  topic: 'karafka_topic',
  before_message: -> { any_class_name_instance_method },
  after_message: ->(result) { "This is result of method run: #{result}" }
)
```

now each time you run:

```ruby
ClassName.new.run
```

a message with the given message will be send before and after the method execution.

## References

* [Karafka framework](https://github.com/karafka/karafka)
* [Waterdrop](https://github.com/karafka/waterdrop)
* [Sidekiq Glass](https://github.com/karafka/sidekiq-glass)
* [Envlogic](https://github.com/karafka/envlogic)
* [Null Logger](https://github.com/karafka/null-logger)
* [WaterDrop Travis CI](https://travis-ci.org/karafka/waterdrop)
* [WaterDrop Code Climate](https://codeclimate.com/github/karafka/waterdrop)

## Note on Patches/Pull Requests

Fork the project.
Make your feature addition or bug fix.
Add tests for it. This is important so I don't break it in a future version unintentionally.
Commit, do not mess with Rakefile, version, or history. (if you want to have your own version, that is fine but bump version in a commit by itself I can ignore when I pull). Send me a pull request. Bonus points for topic branches.

Each pull request must pass our quality requirements. To check if everything is as it should be, we use [PolishGeeks Dev Tools](https://github.com/polishgeeks/polishgeeks-dev-tools) that combine multiple linters and code analyzers. Please run:

```bash
bundle exec rake
```

to check if everything is in order. After that you can submit a pull request.
