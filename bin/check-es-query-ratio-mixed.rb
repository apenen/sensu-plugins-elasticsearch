#! /usr/bin/env ruby
#
#   check-es-query-ratio-mixed
#
# DESCRIPTION:
#   This plugin checks ratio between results of two Elasticsearch queries, now with aggregations (or not)
#
# OUTPUT:
#   plain text
#
# PLATFORMS:
#   Linux
#
# DEPENDENCIES:
#   gem: sensu-plugin
#   gem: elasticsearch
#   gem: aws_es_transport
#
# USAGE:
#   This example checks the ratio from the count/search of two different queries
#   as dividend and divisor at the host elasticsearch.service.consul for the past 90 minutes
#   will warn if percentage is lower than 10 and critical if percentage is lower than 5
#   (The invert flag warns if results are _below_ the critical and warning values)
#   check-es-query-ratio-mixed.rb -h elasticsearch.service.consul -p 9200 -u user -P password
#     -Q "field:search" -I "dividend-index-* -q "*:*" -i "divisor-index-*"
#     -a "divisor_field_aggregation" --divisor-aggr-type cardinality --minutes-previous 90
#     -c 5 -w 10
#
#
# NOTES:
#
# LICENSE:
#
#   Released under the same terms as Sensu (the MIT license); see LICENSE
#   for details.
#

require 'sensu-plugin/check/cli'
require 'elasticsearch'
require 'time'
require 'uri'
require 'aws_es_transport'
require 'sensu-plugins-elasticsearch'

#
# ES Query Count
#
class ESQueryRatioMixed < Sensu::Plugin::Check::CLI
  include ElasticsearchCommon

  option :dividend_index,
         description: 'Elasticsearch indices to query where percentage is calculated for.
         Comma-separated list of index names to search.
         Use `_all` or empty string to perform the operation on all indices.
         Accepts wildcards',
         short: '-I INDEX',
         long: '--dividend-index INDEX'

  option :divisor_index,
         description: 'Elasticsearch indices to query where percentage is calculated from.
         Comma-separated list of index names to search.
         Use `_all` or empty string to perform the operation on all indices.
         Accepts wildcards',
         short: '-i INDEX',
         long: '--divisor-index INDEX'

  option :transport,
         long: '--transport TRANSPORT',
         description: 'Transport to use to communicate with ES. Use "AWS" for signed AWS transports.'

  option :region,
         long: '--region REGION',
         description: 'Region (necessary for AWS Transport)'

  option :dividend_types,
         description: 'Elasticsearch types to limit searches to, comma separated list. Dividend.',
         long: '--dividend-types TYPES'

  option :divisor_types,
         description: 'Elasticsearch types to limit searches to, comma separated list. Divisor.',
         long: '--divisor-types TYPES'

  option :dividend_timestamp_field,
         description: 'Field to use instead of @timestamp for dividend query.',
         long: '--dividend-timestamp-field FIELD_NAME',
         default: '@timestamp'

  option :divisor_timestamp_field,
         description: 'Field to use instead of @timestamp for divisor query.',
         long: '--divisor-timestamp-field FIELD_NAME',
         default: '@timestamp'

  option :offset,
         description: 'Seconds before offset to end @timestamp against query.',
         long: '--offset OFFSET',
         proc: proc(&:to_i),
         default: 0

  option :ignore_unavailable,
         description: 'Ignore unavailable indices.',
         long: '--ignore-unavailable',
         boolean: true,
         default: true

  option :minutes_previous,
         description: 'Minutes before offset to check @timestamp against query.',
         long: '--minutes-previous MINUTES_PREVIOUS',
         proc: proc(&:to_i),
         default: 0

  option :hours_previous,
         description: 'Hours before offset to check @timestamp against query.',
         long: '--hours-previous HOURS_PREVIOUS',
         proc: proc(&:to_i),
         default: 0

  option :days_previous,
         description: 'Days before offset to check @timestamp against query.',
         long: '--days-previous DAYS_PREVIOUS',
         proc: proc(&:to_i),
         default: 0

  option :weeks_previous,
         description: 'Weeks before offset to check @timestamp against query.',
         long: '--weeks-previous WEEKS_PREVIOUS',
         proc: proc(&:to_i),
         default: 0

  option :months_previous,
         description: 'Months before offset to check @timestamp against query.',
         long: '--months-previous MONTHS_PREVIOUS',
         proc: proc(&:to_i),
         default: 0

  option :dividend_date_index,
         description: 'Elasticsearch time based index for dividend.
            Accepts format from http://ruby-doc.org/core-2.2.0/Time.html#method-i-strftime',
         short: '-D DATE_INDEX',
         long: '--dividend-date-index DATE_INDEX'

  option :divisor_date_index,
         description: 'Elasticsearch time based index for divisor.
            Accepts format from http://ruby-doc.org/core-2.2.0/Time.html#method-i-strftime',
         short: '-d DATE_INDEX',
         long: '--divisor-date-index DATE_INDEX'

  option :date_repeat_daily,
         description: 'Elasticsearch date based index repeats daily.',
         long: '--repeat-daily',
         boolean: true,
         default: true

  option :date_repeat_hourly,
         description: 'Elasticsearch date based index repeats hourly.',
         long: '--repeat-hourly',
         boolean: true,
         default: false

  option :dividend_search_field,
         description: 'The Elasticsearch document field to search for your dividend query string.',
         short: '-F FIELD',
         long: '--dividend-field FIELD',
         required: false,
         default: 'message'

  option :divisor_search_field,
         description: 'The Elasticsearch document field to search for your divisor query string.',
         short: '-f FIELD',
         long: '--divisor-field FIELD',
         required: false,
         default: 'message'

  option :dividend_query,
         description: 'Elasticsearch query where percentage is calculated for',
         short: '-Q QUERY',
         long: '--dividend-query QUERY',
         required: true

  option :divisor_query,
         description: 'Elasticsearch query where percentage is calculated from',
         short: '-q QUERY',
         long: '--divisor-query QUERY',
         required: true

  option :dividend_aggr_type,
         description: 'Elasticsearch query dividend aggr type',
         long: '--dividend-aggr-type TYPE',
         required: false

  option :divisor_aggr_type,
         description: 'Elasticsearch query divisor aggr type',
         long: '--divisor-aggr-type TYPE',
         required: false

  option :dividend_aggr_field,
         description: 'Elasticsearch query field to aggregate and average for',
         short: '-A FIELD',
         long: '--dividend-aggr-field FIELD',
         required: false

  option :divisor_aggr_field,
         description: 'Elasticsearch query field to aggregate and average from',
         short: '-a FIELD',
         long: '--divisor-aggr-field FIELD',
         required: false

  option :host,
         description: 'Elasticsearch host',
         short: '-h HOST',
         long: '--host HOST',
         default: 'localhost'

  option :port,
         description: 'Elasticsearch port',
         short: '-p PORT',
         long: '--port PORT',
         proc: proc(&:to_i),
         default: 9200

  option :scheme,
         description: 'Elasticsearch connection scheme, defaults to https for authenticated connections',
         short: '-s SCHEME',
         long: '--scheme SCHEME'

  option :password,
         description: 'Elasticsearch connection password',
         short: '-P PASSWORD',
         long: '--password PASSWORD'

  option :user,
         description: 'Elasticsearch connection user',
         short: '-u USER',
         long: '--user USER'

  option :headers,
         description: 'A comma separated list of headers to pass to elasticsearch http client',
         short: '-H headers',
         long: '--headers headers',
         default: 'Content-Type: application/json'

  option :timeout,
         description: 'Elasticsearch query timeout in seconds',
         short: '-t TIMEOUT',
         long: '--timeout TIMEOUT',
         proc: proc(&:to_i),
         default: 30

  option :warn,
         short: '-w N',
         long: '--warn N',
         description: 'Result count WARNING threshold',
         proc: proc(&:to_f),
         default: 0

  option :crit,
         short: '-c N',
         long: '--crit N',
         description: 'Result count CRITICAL threshold',
         proc: proc(&:to_f),
         default: 0

  option :invert,
         long: '--invert',
         description: 'Invert thresholds',
         boolean: true

  option :divisor_zero_ok,
         short: '-z',
         long: '--zero',
         description: 'Division by 0 returns OK',
         boolean: true,
         default: false

  option :kibana_url,
         long: '--kibana-url KIBANA_URL',
         description: 'Kibana URL query prefix that will be in critical / warning response output.'

  def kibana_info
    kibana_date_format = '%Y-%m-%dT%H:%M:%S.%LZ'
    unless config[:kibana_url].nil?
      index = config[:index]
      unless config[:date_index].nil?
        date_index_partition = config[:date_index].split('%')
        index = "[#{date_index_partition.first}]" \
          "#{date_index_partition[1..-1].join.sub('Y', 'YYYY').sub('y', 'YY').sub('m', 'MM').sub('d', 'DD').sub('j', 'DDDD').sub('H', 'hh')}"
      end
      end_time = Time.now.utc.to_i
      start_time = end_time
      if config[:minutes_previous] != 0
        start_time -= (config[:minutes_previous] * 60)
      end
      if config[:hours_previous] != 0
        start_time -= (config[:hours_previous] * 60 * 60)
      end
      if config[:days_previous] != 0
        start_time -= (config[:days_previous] * 60 * 60 * 24)
      end
      if config[:weeks_previous] != 0
        start_time -= (config[:weeks_previous] * 60 * 60 * 24 * 7)
      end
      if config[:months_previous] != 0
        start_time -= (config[:months_previous] * 60 * 60 * 24 * 31)
      end
      "Kibana logs: #{config[:kibana_url]}/#/discover?_g=" \
      "(refreshInterval:(display:Off,section:0,value:0),time:(from:'" \
      "#{URI.escape(Time.at(start_time).utc.strftime kibana_date_format)}',mode:absolute,to:'" \
      "#{URI.escape(Time.at(end_time).utc.strftime kibana_date_format)}'))&_a=(columns:!(_source),index:" \
      "#{URI.escape(index)},interval:auto,query:(query_string:(analyze_wildcard:!t,query:'" \
      "#{URI.escape(config[:query])}')),sort:!('#{config[:timestamp_field]}',desc))&dummy"
    end
  end

  def run
    # Dividend value
    config[:index] = config[:dividend_index]
    config[:date_index] = config[:dividend_date_index]
    config[:query] = config[:dividend_query]
    config[:search_field] = config[:dividend_search_field]
    config[:type] = config[:dividend_type]
    config[:timestamp_field] = config[:dividend_timestamp_field]
    config.delete(:dividend_index)
    config.delete(:dividend_date_index)
    config.delete(:dividend_query)
    config.delete(:dividend_search_field)
    config.delete(:dividend_type)
    config.delete(:dividend_timestamp_field)
    if config[:dividend_aggr_type].nil || config[:dividend_aggr_field].nil
      response = client.count(build_request_options)
      dividend = response['count']
    else
      config[:aggr] = true
      config[:aggr_name] = 'dividend'
      config[:aggr_type] = config[:dividend_aggr_type]
      config[:aggr_field] = config[:dividend_aggr_field]
      config.delete(:dividend_aggr_type)
      config.delete(:dividend_aggr_field)
      response = client.search(build_request_options)
      dividend = response['aggregations'][config[:aggr_name]]['value']
    end
    # Divisor value
    config[:index] = config[:divisor_index]
    config[:date_index] = config[:divisor_date_index]
    config[:query] = config[:divisor_query]
    config[:search_field] = config[:divisor_search_field]
    config[:type] = config[:divisor_type]
    config[:timestamp_field] = config[:divisor_timestamp_field]
    config.delete(:divisor_index)
    config.delete(:divisor_date_index)
    config.delete(:divisor_query)
    config.delete(:divisor_search_field)
    config.delete(:divisor_type)
    config.delete(:divisor_timestamp_field)
    if config[:divisor_aggr_type].nil || config[:divisor_aggr_field].nil
      response = client.count(build_request_options)
      divisor = response['count']
    else
      config[:aggr] = true
      config[:aggr_name] = 'divisor'
      config[:aggr_type] = config[:divisor_aggr_type]
      config[:aggr_field] = config[:divisor_aggr_field]
      config.delete(:divisor_aggr_type)
      config.delete(:divisor_aggr_field)
      response = client.search(build_request_options)
      divisor = response['aggregations'][config[:aggr_name]]['value']
    end
    divisor_zero_ok = config[:divisor_zero_ok]
    if divisor_zero_ok && divisor.zero?
      ok 'Divisor is 0, ratio check cannot be performed, failing safe with ok'
    elsif divisor.zero?
      critical 'Divisor is 0, ratio check cannot be performed, raising an alert'
    else
      response = {}
      response['count'] = (dividend.to_f / divisor)
    end
    if config[:invert]
      if response['count'] < config[:crit]
        critical "Query count (#{response['count']}) was below critical threshold. #{kibana_info}"
      elsif response['count'] < config[:warn]
        warning "Query count (#{response['count']}) was below warning threshold. #{kibana_info}"
      else
        ok "Query count (#{response['count']}) was ok"
      end
    elsif response['count'] > config[:crit]
      critical "Query count (#{response['count']}) was above critical threshold. #{kibana_info}"
    elsif response['count'] > config[:warn]
      warning "Query count (#{response['count']}) was above warning threshold. #{kibana_info}"
    else
      ok "Query count (#{response['count']}) was ok"
    end
  rescue Elasticsearch::Transport::Transport::Errors::NotFound
    if config[:invert]
      if response['count'] < config[:crit]
        critical "Query count (#{response['count']}) was below critical threshold. #{kibana_info}"
      elsif response['count'] < config[:warn]
        warning "Query count (#{response['count']}) was below warning threshold. #{kibana_info}"
      else
        ok "Query count (#{response['count']}) was ok"
      end
    else
      ok 'No results found, count was below thresholds'
    end
  end
end
