# WaterDrop changelog

## 0.3.2
- Removed support for Ruby 2.1.*
- ~~Ruby 2.3.3 as default~~
- Ruby 2.4.0 as default
- Gem dump x2
- Dry configurable config (#20)
- added .rspec for default spec helper require


## 0.3.1
- Dev tools update
- Gem update
- Specs updates
- File namming convention fix from waterdrop to water_drop + compatibility file
- Additional params (partition, etc) that can be passed into producer

## 0.3.0
- Driver change from Poseidon (not maintained) to Ruby-Kafka

## 0.2.0
- Version dump - this WaterDrop version no longer relies on Aspector to work
- #17 - Logger for Aspector - WaterDrop no longer depends on Aspector
- #8 - add send date as a default value added to a message - wontfix. Should be implemented on a message level since WaterDrop just transports messages without adding additional stuff.
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
- Added a proxu layer to to producer so we could replate Kafka with other messaging systems
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
