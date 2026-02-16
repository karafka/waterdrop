# frozen_string_literal: true

# Integration test for WaterDrop OAuth token refresh functionality
# Tests that the oauthbearer.token_refresh event is properly triggered
# and that tokens can be set via the bearer callback.
#
# This test requires:
# - Keycloak running on port 8080 with the kafka realm configured
# - Kafka with SASL_PLAINTEXT/OAUTHBEARER on port 9094
#
# Run with: docker compose -f docker-compose.oauth.yml up -d
# Then: ./bin/integrations oauth_token_refresh

require "waterdrop"
require "net/http"
require "json"

KEYCLOAK_URL = "http://127.0.0.1:8080"
KAFKA_BOOTSTRAP = "127.0.0.1:9094"
CLIENT_ID = "waterdrop-test"
CLIENT_SECRET = "test-secret"

# Track OAuth callback invocations
oauth_callbacks = []
token_set_success = false

# Token fetcher for Keycloak
def fetch_token
  uri = URI("#{KEYCLOAK_URL}/realms/kafka/protocol/openid-connect/token")
  response = Net::HTTP.post_form(uri, {
    "grant_type" => "client_credentials",
    "client_id" => CLIENT_ID,
    "client_secret" => CLIENT_SECRET
  })
  JSON.parse(response.body)
end

producer = WaterDrop::Producer.new do |config|
  config.kafka = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "security.protocol": "SASL_PLAINTEXT",
    "sasl.mechanism": "OAUTHBEARER"
  }
end

# Subscribe to OAuth token refresh callback
producer.monitor.subscribe("oauthbearer.token_refresh") do |event|
  oauth_callbacks << Time.now

  token_data = fetch_token

  event[:bearer].oauthbearer_set_token(
    token: token_data["access_token"],
    lifetime_ms: token_data["expires_in"] * 1000,
    principal_name: CLIENT_ID
  )

  token_set_success = true
end

# Attempt to produce - this triggers OAuth flow
begin
  producer.produce_sync(topic: "oauth-test", payload: "test message")
rescue => e
  puts "Production failed: #{e.message}"
  producer.close
  exit 1
end

producer.close

# Verify OAuth callback was invoked
if oauth_callbacks.empty?
  puts "FAIL: OAuth callback was never invoked"
  exit 1
end

if !token_set_success
  puts "FAIL: Token was not set successfully"
  exit 1
end

puts "SUCCESS: OAuth callback invoked #{oauth_callbacks.size} time(s)"
exit 0
