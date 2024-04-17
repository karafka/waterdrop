# WaterDrop changelog

## 2.7.0 (Unreleased)

This release contains **BREAKING** changes. Make sure to read and apply upgrade notes.

- **[Feature]** Support custom OAuth providers.
- **[Breaking]** Drop Ruby `2.7` support.
- **[Breaking]** Change default timeouts so final delivery `message.timeout.ms` is less that `max_wait_time` so we do not end up with not final verdict.
- **[Breaking]** Update all the time related configuration settings to be in `ms` and not mixed.
- **[Breaking]** Remove no longer needed `wait_timeout` configuration option.
- **[Breaking]** Do **not** validate or morph (via middleware) messages added to the buffer prior to `flush_sync` or `flush_async`.
- [Enhancement] Provide `WaterDrop::Producer#transaction?` that returns only when producer has an active transaction running.
- [Enhancement] Introduce `instrument_on_wait_queue_full` flag (defaults to `true`) to be able to configure whether non critical (retryable) queue full errors should be instrumented in the error pipeline. Useful when building high-performance pipes with WaterDrop queue retry backoff as a throttler.
- [Fix] Fix a case where purge on non-initialized client would crash.
- [Fix] Middlewares run twice when using buffered produce.
- [Fix] Validations run twice when using buffered produce.

### Upgrade Notes

**PLEASE MAKE SURE TO READ AND APPLY THEM!**

#### `wait_timeout` Configuration No Longer Needed

The `wait_timeout` WaterDrop configuration option is no longer needed. You can safely remove it.

```ruby
producer = WaterDrop::Producer.new

producer.setup do |config|
  # Other config...

  # Remove this, no longer needed
  config.wait_timeout = 30
end
```

#### Time Settings Format Alignment

**All** time-related values are now configured in milliseconds instead of some being in seconds and some in milliseconds.

The values that were changed from seconds to milliseconds are:

- `max_wait_timeout`
- `wait_backoff_on_queue_full`
- `wait_timeout_on_queue_full`
- `wait_backoff_on_transaction_command, default`

If you have configured any of those yourself, please replace the seconds representation with milliseconds:

```ruby
producer = WaterDrop::Producer.new

producer.setup do |config|
  config.deliver = true

  # Replace this:
  config.max_wait_timeout = 30

  # With
  config.max_wait_timeout = 30_000
  # ...
end
```

#### Defaults Alignment

In this release, we've updated our default settings to address a crucial issue: previous defaults could lead to inconclusive outcomes in synchronous operations due to wait timeout errors. Users often mistakenly believed that a message dispatch was halted because of these errors when, in fact, the timeout was related to awaiting the final dispatch verdict, not the dispatch action itself.

The new defaults in WaterDrop 2.7.0 eliminate this confusion by ensuring synchronous operation results are always transparent and conclusive. This change aims to provide a straightforward understanding of wait timeout errors, reinforcing that they reflect the wait state, not the dispatch success.

Below, you can find a table with what has changed, the new defaults, and the current ones in case you want to retain the previous behavior:

<table>
  <thead>
    <tr>
      <th>Config</th>
      <th>Previous Default</th>
      <th>New Default</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>root <code>max_wait_timeout</code></td>
      <td>5000 ms (5 seconds)</td>
      <td>60000 ms (60 seconds)</td>
    </tr>
    <tr>
      <td>kafka <code>message.timeout.ms</code></td>
      <td>300000 ms (5 minutes)</td>
      <td>50000 ms (50 seconds)</td>
    </tr>
    <tr>
      <td>kafka <code>transaction.timeout.ms</code></td>
      <td>60000 ms (1 minute)</td>
      <td>55000 ms (55 seconds)</td>
    </tr>
  </tbody>
</table>

This alignment ensures that when using sync operations or invoking `#wait`, any exception you get should give you a conclusive and final delivery verdict.

#### Buffering No Longer Early Validates Messages

As of version `2.7.0`, WaterDrop has changed how message buffering works. Previously, messages underwent validation and middleware processing when they were buffered. Now, these steps are deferred until just before dispatching the messages. The buffer functions strictly as a thread-safe storage area without performing any validations or middleware operations until the messages are ready to be sent.

This adjustment was made primarily to ensure that middleware runs and validations are applied when most relevant—shortly before message dispatch. This approach addresses potential issues with buffers that might hold messages for extended periods:

- **Temporal Relevance**: Validating and processing messages near their dispatch time helps ensure that actions such as partition assignments reflect the current system state. This is crucial in dynamic environments where system states are subject to rapid changes.

- **Stale State Management**: By delaying validations and middleware to the dispatch phase, the system minimizes the risk of acting on outdated information, which could lead to incorrect processing or partitioning decisions.

```ruby
# Prior to 2.7.0 this would raise an error
producer.buffer(topic: nil, payload: '')
# => WaterDrop::Errors::MessageInvalidError

# After 2.7.0 buffer will not, but flush_async will
producer.buffer(topic: nil, payload: '')
# => all good here
producer.flush_async(topic: nil, payload: '')
# => WaterDrop::Errors::MessageInvalidError
```

#### Middleware Execution Prior to Flush When Buffering

The timing of middleware execution has been adjusted. Middleware, which was previously run when messages were added to the buffer, will now only execute immediately before the messages are flushed from the buffer and dispatched. This change is similar to the validation-related changes.

## 2.6.14 (2024-02-06)
- [Enhancement] Instrument `producer.connected` and `producer.closing` lifecycle events.

## 2.6.13 (2024-01-29)
- [Enhancement] Expose `#partition_count` for building custom partitioners that need to be aware of number of partitions on a given topic.

## 2.6.12 (2024-01-03)
- [Enhancement] Provide ability to label message dispatches for increased observability.
- [Enhancement] Provide ability to commit offset during the transaction with a consumer provided.
- [Change] Change transactional message purged error type from `message.error` to `librdkafka.dispatch_error` to align with the non-transactional error type.
- [Change] Remove usage of concurrent ruby.

## 2.6.11 (2023-10-25)
- [Enhancement] Return delivery handles and delivery report for both dummy and buffered clients with proper topics, partitions and offsets assign and auto-increment offsets per partition.
- [Fix] Fix a case where buffered test client would not accumulate messages on failed transactions

## 2.6.10 (2023-10-24)
- [Improvement] Introduce `message.purged` event to indicate that a message that was not delivered to Kafka was purged. This most of the time refers to messages that were part of a transaction and were not yet dispatched to Kafka. It always means, that given message was not delivered but in case of transactions it is expected. In case of non-transactional it usually means `#purge` usage or exceeding `message.timeout.ms` so `librdkafka` removes this message from its internal queue. Non-transactional producers do **not** use this and pipe purges to `error.occurred`.
- [Fix] Fix a case where `message.acknowledged` would not have `caller` key.
- [Fix] Fix a bug where critical errors (like `IRB::Abort`) would not abort the ongoing transaction.

## 2.6.9 (2023-10-23)
- [Improvement] Introduce a `transaction.finished` event to indicate that transaction has finished whether it was aborted or committed.
- [Improvement] Use `transaction.committed` event to indicate that transaction has been committed.

## 2.6.8 (2023-10-20)
- **[Feature]** Introduce transactions support.
- [Improvement] Expand `LoggerListener` to inform about transactions (info level).
- [Improvement] Allow waterdrop to use topic as a symbol or a string.
- [Improvement] Enhance both `message.acknowledged` and `error.occurred` (for `librdkafka.dispatch_error`) with full delivery_report.
- [Improvement] Provide `#close!` that will force producer close even with outgoing data after the ma wait timeout.
- [Improvement] Provide `#purge` that will purge any outgoing data and data from the internal queues (both WaterDrop and librdkafka).
- [Fix] Fix the `librdkafka.dispatch_error` error dispatch for errors with negative code.

## 2.6.7 (2023-09-01)
- [Improvement] early flush data from `librdkafka` internal buffer before closing.
- [Maintenance] Update the signing cert as the old one expired.

## 2.6.6 (2023-08-03)
- [Improvement] Provide `log_messages` option to `LoggerListener` so the extensive messages data logging can disabled.

## 2.6.5 (2023-07-22)
- [Fix] Add cause to the errors that are passed into instrumentation (konalegi)

## 2.6.4 (2023-07-11)
- [Improvement] Use original error `#inspect` for `WaterDrop::Errors::ProduceError` and `WaterDrop::Errors::ProduceManyError` instead of the current empty string.

## 2.6.3 (2023-06-28)
- [Change] Use `Concurrent::AtomicFixnum` to track operations in progress to prevent potential race conditions on JRuby and TruffleRuby (not yet supported but this is for future usage).
- [Change] Require `karafka-rdkafka` `>= 0.13.2`.
- [Change] Require 'karafka-core' `>= 2.1.1`

## 2.6.2 (2023-06-21)
- [Refactor] Introduce a counter-based locking approach to make sure, that we close the producer safely but at the same time not to limit messages production with producing lock.
- [Refactor] Make private methods private.
- [Refactor] Validate that producer is not closed only when attempting to produce.
- [Refactor] Improve one 5 minute long spec to run in 10 seconds.
- [Refactor] clear client assignment after closing.

## 2.6.1 (2023-06-19)
- [Refactor] Remove no longer needed patches.
- [Fix] Fork detection on a short lived processes seems to fail. Clear the used parent process client reference not to close it in the finalizer (#356).
- [Change] Require `karafka-rdkafka` `>= 0.13.0`.
- [Change] Require 'karafka-core' `>= 2.1.0`

## 2.6.0 (2023-06-11)
- [Improvement] Introduce `client_class` setting for ability to replace underlying client with anything specific to a given env (dev, test, etc).
- [Improvement] Introduce `Clients::Buffered` useful for writing specs that do not have to talk with Kafka (id-ilych)
- [Improvement] Make `#produce` method private to avoid confusion and make sure it is not used directly (it is not part of the official API).
- [Change] Change `wait_on_queue_full` from `false` to `true` as a default.
- [Change] Rename `wait_on_queue_full_timeout` to `wait_backoff_on_queue_full` to match what it actually does.
- [Enhancement] Introduce `wait_timeout_on_queue_full` with proper meaning. That is, this represents time after which despite backoff the error will be raised. This should allow to raise an error in case the backoff attempts were insufficient. This prevents from a case, where upon never deliverable messages we would end up with an infinite loop.
- [Fix] Provide `type` for queue full errors that references the appropriate public API method correctly.

### Upgrade notes

1. Rename `wait_on_queue_full_timeout` to `wait_backoff_on_queue_full`.
2. Set `wait_on_queue_full` to `false` if you did not use it and do not want.

## 2.5.3 (2023-05-26)
- [Enhancement] Include topic name in the `error.occurred` notification payload.
- [Enhancement] Include topic name in the `message.acknowledged` notification payload.
- [Maintenance] Require `karafka-core` `2.0.13`

## 2.5.2 (2023-04-24)
- [Fix] Require missing Pathname (#345)

## 2.5.1 (2023-03-09)
- [Feature] Introduce a configurable backoff upon `librdkafka` queue full (false by default).

## 2.5.0 (2023-03-04)
- [Feature] Pipe **all** the errors including synchronous errors via the `error.occurred`.
- [Improvement] Pipe delivery errors that occurred not via the error callback using the `error.occurred` channel.
- [Improvement] Introduce `WaterDrop::Errors::ProduceError` and `WaterDrop::Errors::ProduceManyError` for any inline raised errors that occur. You can get the original error by using the `#cause`.
- [Improvement] Include `#dispatched` messages handler in the `WaterDrop::Errors::ProduceManyError` error, to be able to understand which of the messages were delegated to `librdkafka` prior to the failure.
- [Maintenance] Remove the `WaterDrop::Errors::FlushFailureError` in favour of correct error that occurred to unify the error handling.
- [Maintenance] Rename `Datadog::Listener` to `Datadog::MetricsListener` to align with Karafka (#329).
- [Fix] Do **not** flush when there is no data to flush in the internal buffer.
- [Fix] Wait on the final data flush for short-lived producers to make sure, that the message is actually dispatched by `librdkafka` or timeout.

### Upgrade notes

Please note, this **is** a **breaking** release, hence `2.5.0`.

1. If you used to catch `WaterDrop::Errors::FlushFailureError` now you need to catch `WaterDrop::Errors::ProduceError`. `WaterDrop::Errors::ProduceManyError` is based on the `ProduceError`, hence it should be enough.
2. Prior to `2.5.0` there was always a chance of partial dispatches via `produce_many_` methods. Now you can get the info on all the errors via `error.occurred`.
3. Inline `Rdkafka::RdkafkaError` are now re-raised via `WaterDrop::Errors::ProduceError` and available under `#cause`. Async `Rdkafka::RdkafkaError` errors are still directly available and you can differentiate between errors using the event `type`.
4. If you are using the Datadog listener, you need to:

```ruby
# Replace require:
require 'waterdrop/instrumentation/vendors/datadog/listener'
# With
require 'waterdrop/instrumentation/vendors/datadog/metrics_listener'

# Replace references of
::WaterDrop::Instrumentation::Vendors::Datadog::Listener.new
# With
::WaterDrop::Instrumentation::Vendors::Datadog::MetricsListener.new
```

## 2.4.11 (2023-02-24)
- Replace the local rspec locator with generalized core one.
- Make `::WaterDrop::Instrumentation::Notifications::EVENTS` list public for anyone wanting to re-bind those into a different notification bus.

## 2.4.10 (2023-01-30)
- Include `caller` in the error instrumentation to align with Karafka.

## 2.4.9 (2023-01-11)
- Remove empty debug logging out of `LoggerListener`.
- Do not lock Ruby version in Karafka in favour of `karafka-core`.
- Make sure `karafka-core` version is at least `2.0.9` to make sure we run `karafka-rdkafka`.

## 2.4.8 (2023-01-07)
- Use monotonic time from Karafka core.

## 2.4.7 (2022-12-18)
- Add support to customizable middlewares that can modify message hash prior to validation and dispatch.
- Fix a case where upon not-available leader, metadata request would not be retried
- Require `karafka-core` 2.0.7.

## 2.4.6 (2022-12-10)
- Set `statistics.interval.ms` to 5 seconds by default, so the defaults cover all the instrumentation out of the box.

### Upgrade notes

If you want to disable `librdkafka` statistics because you do not use them at all, update the `kafka` `statistics.interval.ms` setting and set it to `0`:

```ruby
producer = WaterDrop::Producer.new

producer.setup do |config|
  config.deliver = true
  config.kafka = {
    'bootstrap.servers': 'localhost:9092',
    'statistics.interval.ms': 0
  }
end
```

## 2.4.5 (2022-12-10)
- Fix invalid error scope visibility.
- Cache partition count to improve messages production and lower stress on Kafka when `partition_key` is on.

## 2.4.4 (2022-12-09)
- Add temporary patch on top of `rdkafka-ruby` to mitigate metadata fetch timeout failures.

## 2.4.3 (2022-12-07)
- Support for librdkafka 0.13
- Update Github Actions
- Change auto-generated id from `SecureRandom#uuid` to `SecureRandom#hex(6)`
- Remove shared components that were moved to `karafka-core` from WaterDrop

## 2.4.2 (2022-09-29)
- Allow sending tombstone messages (#267)

## 2.4.1 (2022-08-01)
- Replace local statistics decorator with the one extracted to `karafka-core`.

## 2.4.0 (2022-07-28)
- Small refactor of the DataDog/Statsd listener to align for future extraction to `karafka-common`.
- Replace `dry-monitor` with home-brew notification layer (API compatible) and allow for usage with `ActiveSupport::Notifications`.
- Remove all the common code into `karafka-core` and add it as a dependency.

## 2.3.3 (2022-07-18)
- Replace `dry-validation` with home-brew validation layer and drop direct dependency on `dry-validation`.
- Remove indirect dependency on dry-configurable from DataDog listener (no changes required).

## 2.3.2 (2022-07-17)
- Replace `dry-configurable` with home-brew config and drop direct dependency on `dry-configurable`.

## 2.3.1 (2022-06-17)
- Update rdkafka patches to align with `0.12.0` and `0.11.1` support.

## 2.3.0 (2022-04-03)
-  Rename StdoutListener to LoggerListener (#240)

## 2.2.0 (2022-02-18)
- Add Datadog listener for metrics + errors publishing
- Add Datadog example dashboard template
- Update Readme to show Dd instrumentation usage
- Align the directory namespace convention with gem name (waterdrop => WaterDrop)
- Introduce a common base for validation contracts
- Drop CI support for ruby 2.6
- Require all `kafka` settings to have symbol keys (compatibility with Karafka 2.0 and rdkafka)

## 2.1.0 (2022-01-03)
- Ruby 3.1 support
- Change the error notification key from `error.emitted` to `error.occurred`.
- Normalize error tracking and make all the places publish errors into the same notification endpoint (`error.occurred`).
- Start semantic versioning WaterDrop.

## 2.0.7 (2021-12-03)
- Source code metadata url added to the gemspec
- Replace `:producer` with `:producer_id` in events and update `StdoutListener` accordingly. This change aligns all the events in terms of not publishing the whole producer object in the events.
- Add `error.emitted` into the `StdoutListener`.
- Enable `StdoutLogger` in specs for additional integration coverage.

## 2.0.6 (2021-12-01)
- #218 - Fixes a case, where dispatch of callbacks the same moment a new producer was created could cause a concurrency issue in the manager.
- Fix some unstable specs.

## 2.0.5 (2021-11-28)

### Bug fixes

- Fixes an issue where multiple producers would emit stats of other producers causing the same stats to be published several times (as many times as a number of producers). This could cause invalid reporting for multi-kafka setups.
- Fixes a bug where emitted statistics would contain their first value as the first delta value for first stats emitted.
- Fixes a bug where decorated statistics would include a delta for a root field with non-numeric values.

### Changes and features
- Introduces support for error callbacks instrumentation notifications with `error.emitted` monitor emitted key for tracking background errors that would occur on the producer (disconnects, etc).
- Removes the `:producer` key from `statistics.emitted` and replaces it with `:producer_id` not to inject whole producer into the payload
- Removes the `:producer` key from `message.acknowledged` and replaces it with `:producer_id` not to inject whole producer into the payload
- Cleanup and refactor of callbacks support to simplify the API and make it work with Rdkafka way of things.
- Introduces a callbacks manager concept that will also be within in Karafka `2.0` for both statistics and errors tracking per client.
- Sets default Kafka `client.id` to `waterdrop` when not set.
- Updates specs to always emit statistics for better test coverage.
- Adds statistics and errors integration specs running against Kafka.
- Replaces direct `RSpec.describe` reference with auto-discovery
- Patches `rdkafka` to provide functionalities that are needed for granular callback support.

## 2.0.4 (2021-09-19)
- Update `dry-*` to the recent versions and update settings syntax to match it
- Update Zeitwerk requirement

## 2.0.3 (2021-09-05)
- Remove rdkafka patch in favour of spec topic pre-creation
- Do not close client that was never used upon closing producer

## 2.0.2 (2021-08-13)
- Add support for `partition_key`
- Switch license from `LGPL-3.0` to  `MIT`
- Switch flushing on close to sync

## 2.0.1 (2021-06-05)
- Remove Ruby 2.5 support and update minimum Ruby requirement to 2.6
- Fix the `finalizer references object to be finalized` warning issued with 3.0

## 2.0.0 (2020-12-13)
- Redesign of the whole API (see `README.md` for the use-cases and the current API)
- Replace `ruby-kafka` with `rdkafka`
- Switch license from `MIT` to `LGPL-3.0`
- #113 - Add some basic validations of the kafka scope of the config (Azdaroth)
- Global state removed
- Redesigned metrics that use `rdkafka` internal data + custom diffing
- Restore JRuby support

## 1.4.0 (2020-08-25)
- Release to match Karafka 1.4 versioning.

## 1.3.4 (2020-02-17)
- Support for new `dry-configurable`

## 1.3.3 (2019-01-06)
- #119 - Support exactly once delivery and transactional messaging (kylekthompson)
- #119 - Support delivery_boy 1.0 (kylekthompson)

## 1.3.2 (2019-26-12)
- Ruby 2.7.0 support
- Fix missing `delegate` dependency on `ruby-kafka`

## 1.3.1 (2019-10-21)
- Ruby 2.6.5 support
- Expose setting to optionally verify hostname on ssl certs #109 (tabdollahi)

## 1.3.0 (2019-09-09)
- Drop Ruby 2.4 support

## 1.3.0.rc1 (2019-07-31)
- Drop Ruby 2.3 support
- Drop support for Kafka 0.10 in favor of native support for Kafka 0.11.
- Ruby 2.6.3 support
- Support message headers
- `sasl_over_ssl` support
- Unlock Ruby Kafka + provide support for 0.7 only
- #60 - Rename listener to StdoutListener
- Drop support for Kafka 0.10 in favor of native support for Kafka 0.11.
- Support ruby-kafka 0.7
- Support message headers
- `sasl_over_ssl` support
- `ssl_client_cert_key_password` support
- #87 - Make stdout listener as instance
- Use Zeitwerk for gem code loading
- #93 - zstd compression support
- #99 - schemas are renamed to contracts
- Bump delivery_boy (0.2.7 => 0.2.8)

## 1.2.5
- Bump dependencies to match Karafka
- drop jruby support
- drop ruby 2.2 support

## 1.2.4
- Due to multiple requests, unlock of 0.7 with an additional post-install message

## 1.2.3
- Lock ruby-kafka to 0.6 (0.7 support targeted for WaterDrop 1.3)

## 1.2.2
- #55 - Codec settings unification and config applier

## 1.2.1
- #54 - compression_codec api sync with king-konf requirements

## 1.2.0
- #45 - Allow specifying a create time for messages
- #47 - Support SCRAM once released
- #49 - Add lz4 support once merged and released
- #50 - Potential message loss in async mode
- Ruby 2.5.0 support
- Gem bump to match Karafka framework versioning
- #48 - ssl_ca_certs_from_system
- #52 - Use instrumentation compatible with Karafka 1.2

## 1.0.1
- Added high level retry on connection problems

## 1.0.0

- #37 - ack level for producer
- Gem bump
- Ruby 2.4.2 support
- Raw ruby-kafka driver is now replaced with delivery_boy
- Sync and async producers
- Complete update of the API
- Much better validations for config details
- Complete API remodel - please read the new README
- Renamed send_messages to deliver

## 0.4
- Bump to match Karafka
- Renamed ```hosts``` to ```seed_brokers```
- Removed the ```ssl``` scoping for ```kafka``` config namespace to better match Karafka conventions
- Added ```client_id``` option on a root config level
- Added ```logger``` option on a root config level
- Auto Propagation of config down to ruby-kafka

## 0.3.2
- Removed support for Ruby 2.1.*
- ~~Ruby 2.3.3 as default~~
- Ruby 2.4.0 as default
- Gem dump x2
- Dry configurable config (#20)
- added .rspec for default spec helper require
- Added SSL capabilities
- Coditsu instead of PG dev tools for quality control

## 0.3.1
- Dev tools update
- Gem update
- Specs updates
- File naming convention fix from waterdrop to water_drop + compatibility file
- Additional params (partition, etc) that can be passed into producer

## 0.3.0
- Driver change from Poseidon (not maintained) to Ruby-Kafka

## 0.2.0
- Version dump - this WaterDrop version no longer relies on Aspector to work
- #17 - Logger for Aspector - WaterDrop no longer depends on Aspector
- #8 - add send date as a default value added to a message - wont-fix. Should be implemented on a message level since WaterDrop just transports messages without adding additional stuff.
- #11 - same as above

## 0.1.13
- Resolved bug #15. When you use waterdrop in aspect way, message will be automatically parse to JSON.

## 0.1.12
- Removed default to_json casting because of binary/other data types incompatibility. This is an incompatibility. If you use WaterDrop, please add a proper casting method to places where you use it.
- Gem dump

## 0.1.11
- Poseidon options extractions and tweaks

## 0.1.10
- Switched raise_on_failure to ignore all StandardError failures (or not to), not just specific once
- Reloading inside connection pool connection that seems to be broken (one that failed) - this should prevent from multiple issues (but not from single one) that are related to the connection

## 0.1.9
- Required acks and set to -1 (most secure but slower)
- Added a proxy layer to to producer so we could replace Kafka with other messaging systems
- Gem dump

## 0.1.8
- proper poseidon clients names (not duplicated)

## 0.1.7
- kafka_host, kafka_hosts and kafka_ports settings consistency fix

## 0.1.6
- Added null-logger gem

## 0.1.5
- raise_on_failure flag to ignore (if false) that message was not sent

## 0.1.4
- Renamed WaterDrop::Event to WaterDrop::Message to follow Apache Kafka naming convention

## 0.1.3
- Gems cleanup
- Requirements fix

## 0.1.2
- Initial gem release
