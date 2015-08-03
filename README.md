# WaterDrop

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
