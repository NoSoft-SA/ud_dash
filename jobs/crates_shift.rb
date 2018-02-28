# frozen_string_literal: true

day_query = <<~SQL
  SELECT resources.resource_code AS packhouse_code, shift_types.day_night_or_custom,
    SUM(commodities.equivalent_bins * CASE WHEN bins.tipped_date_time <= (date_trunc('day', localtimestamp) + '10:00:00') THEN 1 ELSE 0 END) as count_10am,
    200 - SUM(commodities.equivalent_bins * CASE WHEN bins.tipped_date_time <= (date_trunc('day', localtimestamp) + '10:00:00') THEN 1 ELSE 0 END) as short_10am,
    SUM(commodities.equivalent_bins * CASE WHEN bins.tipped_date_time <= (date_trunc('day', localtimestamp) + '14:00:00') THEN 1 ELSE 0 END) as count_14pm,
    350 - SUM(commodities.equivalent_bins * CASE WHEN bins.tipped_date_time <= (date_trunc('day', localtimestamp) + '14:00:00') THEN 1 ELSE 0 END) as short_14pm,
    SUM(commodities.equivalent_bins * CASE WHEN bins.tipped_date_time <= (date_trunc('day', localtimestamp) + '17:00:00') THEN 1 ELSE 0 END) as count_17pm,
    750 - SUM(commodities.equivalent_bins * CASE WHEN bins.tipped_date_time <= (date_trunc('day', localtimestamp) + '17:00:00') THEN 1 ELSE 0 END) as short_17pm
  FROM bins
  JOIN shifts ON shifts.id = bins.shift_id
  JOIN shift_types ON shift_types.id = shifts.shift_type_id
  JOIN resources ON resources.id = shift_types.packhouse_resource_id
  JOIN rmt_products ON rmt_products.id = bins.rmt_product_id
  JOIN rmt_varieties ON rmt_varieties.id = rmt_products.rmt_variety_id
  JOIN commodities ON commodities.id = rmt_varieties.commodity_id
  WHERE localtimestamp BETWEEN shifts.start_date_time AND shifts.end_date_time
  GROUP BY resources.resource_code, shift_types.day_night_or_custom
SQL

night_query = <<~SQL
  SELECT resources.resource_code AS packhouse_code, shift_types.day_night_or_custom,
    SUM(commodities.equivalent_bins * CASE WHEN bins.tipped_date_time <= (date_trunc('day', 'pre-midnight'::timestamp) + '21:00:00') THEN 1 ELSE 0 END) as count_21pm,
    200 - SUM(commodities.equivalent_bins * CASE WHEN bins.tipped_date_time <= (date_trunc('day', 'pre-midnight'::timestamp) + '21:00:00') THEN 1 ELSE 0 END) as short_21pm,
    SUM(commodities.equivalent_bins * CASE WHEN bins.tipped_date_time <= (date_trunc('day', 'post-midnight'::timestamp) + '00:20:00') THEN 1 ELSE 0 END) as count_0020am,
    350 - SUM(commodities.equivalent_bins * CASE WHEN bins.tipped_date_time <= (date_trunc('day', 'post-midnight'::timestamp) + '00:20:00') THEN 1 ELSE 0 END) as short_0020am,
    SUM(commodities.equivalent_bins * CASE WHEN bins.tipped_date_time <= (date_trunc('day', 'post-midnight'::timestamp) + '03:00:00') THEN 1 ELSE 0 END) as count_3am,
    750 - SUM(commodities.equivalent_bins * CASE WHEN bins.tipped_date_time <= (date_trunc('day', 'post-midnight'::timestamp) + '03:00:00') THEN 1 ELSE 0 END) as short_3am
  FROM bins
  JOIN shifts ON shifts.id = bins.shift_id
  JOIN shift_types ON shift_types.id = shifts.shift_type_id
  JOIN resources ON resources.id = shift_types.packhouse_resource_id
  JOIN rmt_products ON rmt_products.id = bins.rmt_product_id
  JOIN rmt_varieties ON rmt_varieties.id = rmt_products.rmt_variety_id
  JOIN commodities ON commodities.id = rmt_varieties.commodity_id
  WHERE localtimestamp BETWEEN shifts.start_date_time AND shifts.end_date_time
  GROUP BY resources.resource_code, shift_types.day_night_or_custom
SQL

# :first_in sets how long it takes before the job is first run.
# In this case, it is run immediately
SCHEDULER.every '1m', first_in: 0 do
  now = Time.now
  daytime = true
  if now.hour > 17
    daytime = false
    nxt = now + 1 * 60 * 60 * 24
    query = night_query
            .gsub('pre-midnight', "#{now.year}-#{now.month}-#{now.day} 00:00:00")
            .gsub('post-midnight', "#{nxt.year}-#{nxt.month}-#{nxt.day} 00:00:00")
  elsif now.hour < 7
    daytime = false
    prev = now - 1 * 60 * 60 * 24
    query = night_query
            .gsub('pre-midnight', "#{prev.year}-#{prev.month}-#{prev.day} 00:00:00")
            .gsub('post-midnight', "#{now.year}-#{now.month}-#{now.day} 00:00:00")
  else
    # query = day_query
    # FOR TESTING:
    query = day_query.gsub('localtimestamp', "'2017-10-12 14:00:00'::timestamp")
  end
  res = DBUD[query].all

  res.each do |rec|
    phc = rec[:packhouse_code]
    next unless PACKHOUSES_TO_USE.include?(phc)
    if daytime
      # send bar with day values
      send_event("crateschart_#{phc}",
                 points: [
                   %w[Time Crates Short],
                   ['10-00',  rec[:count_10am].to_f, rec[:short_10am].to_f],
                   ['14-00',  rec[:count_14pm].to_f, rec[:short_14pm].to_f],
                   ['17-00',  rec[:count_17pm].to_f, rec[:short_17pm].to_f]
                 ])
    else
      # send bar with night values
      send_event("crateschart_#{phc}",
                 points: [
                   %w[Time Crates Short],
                   ['21-00',  rec[:count_21pm].to_f, rec[:short_21pm].to_f],
                   ['00-20',  rec[:count_0020am].to_f, rec[:short_0020am].to_f],
                   ['03-00',  rec[:count_3am].to_f, rec[:short_3am].to_f]
                 ])
    end
  end
end
