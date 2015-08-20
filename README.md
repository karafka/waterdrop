# WaterDrop

[![Build Status](https://travis-ci.org/karafka/waterdrop.png)](https://travis-ci.org/karafka/waterdrop)
[![Code Climate](https://codeclimate.com/github/karafka/waterdrop/badges/gpa.svg)](https://codeclimate.com/github/karafka/waterdrop)

Gem used to send events to Kafka in a standard and in an aspect way.

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

| Option                  | Value type    | Description                    |
|-------------------------|---------------|--------------------------------|
| send_events             | Boolean       | Should we send events to Kafka |
| kafka_host              | String        | Kafka server host              |
| kafka_ports             | Array<String> | Kafka server ports             |
| connection_pool_size    | Integer       | Kafka connection pool size     |
| connection_pool_timeout | Integer       | Kafka connection pool timeout  |

To apply this configuration, you need to use a *setup* method:

```ruby
WaterDrop.setup do |config|
  config.send_events = true
  config.connection_pool_size = 20
  config.connection_pool_timeout = 1
  config.kafka_ports = %w( 9092 )
  config.kafka_host = 'localhost'
end
```

This configuration can be placed in *config/initializers* and can vary based on the environment:

```ruby
WaterDrop.setup do |config|
  config.send_events = Rails.env.production?
  config.connection_pool_size = 20
  config.connection_pool_timeout = 1
  config.kafka_ports = %w( 9092 )
  config.kafka_host = Rails.env.production? ? 'prod-host' : 'localhost'
end
```

## Usage

### Creating and sending standard events

To send Kafka messages, you don't need to use aspects, you can create and send events directly:

```ruby
event = WaterDrop::Event.new('topic', 'message')
event.send!
```

message that you want to send should be either castable to string or to json. If it can be casted to both, it will be casted to json.

### Using aspects to handle events

WaterDrop uses [Aspector](https://github.com/gcao/aspector) to allow aspect oriented events hookup. If you need extensive details about aspector usage, please refer to the [examples](https://github.com/gcao/aspector/tree/master/examples) directory of this project.

In general aspects allows adding additional behavior to existing code without modifying the code itself. This way we can create and send events, without "polluting" the business logic with it.

All the WaterDrop aspects accept following parameters:

| Option                  | Value type            | Description                                  |
|-------------------------|-----------------------|----------------------------------------------|
| ClassName               | Class                 | Class to which we want to hook               |
| method: :method_name    | Symbol, Array<Symbol> | Method (or methods) to which we want to hook |
| topic: 'karafka_topic'  | String, Symbol        | Kafka topic to which we will send the event  |

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

an event with the given message will be send to Kafka.

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

an event with the given message will be send to Kafka.

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

an event with the given message will be send before and after the method execution.

## Note on Patches/Pull Requests

Fork the project. Make your feature addition or bug fix. Add tests for it. This is important so I don't break it in a future version unintentionally. Commit, do not mess with Rakefile, version, or history. (if you want to have your own version, that is fine but bump version in a commit by itself I can ignore when I pull) Send me a pull request. Bonus points for topic branches.
