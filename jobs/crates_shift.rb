# frozen_string_literal: true

# Extra view: single bar.

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
SCHEDULER.every '5m', first_in: 0 do
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
    if ENV['TEST_DASH']
      # FOR TESTING:
      query = day_query.gsub('localtimestamp', "'2018-07-10 11:00:00'::timestamp")
    else
      query = day_query
    end
  end
  res = DBUD[query].all

  time_edge = case now.hour * 60 + now.min
              when 301..600
                '10am'
              when 601..840
                '14pm'
              when 841..1020
                '17pm'
              when 1021..1260
                '21pm'
              when 1261..1440
                '0020am'
              when 0..20 # Same as above
                '0020am'
              when 21..300
                '3am'
              end
  single_count = "count_#{time_edge}".to_sym
  single_short = "short_#{time_edge}".to_sym
  ph_used = []
  res.each do |rec|
    phc = rec[:packhouse_code]
    next unless PACKHOUSES_TO_USE.include?(phc)
    ph_used << phc
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

    # send bar with values of just the current shift
    send_event("crateschart_1_#{phc}",
               points: [
                 %w[Packed Crates Short],
                 ['This Shift', rec[single_count].to_f, rec[single_short].to_f]
               ])
  end

  # Make sure the dashboard is updated with zero-values
  # if there is no data for a packhouse:
  (PACKHOUSES_TO_USE - ph_used).each do |phc|
    send_event("crateschart_#{phc}",
               points: [
                 %w[Time Crates Short],
                 ['10-00',  0.0, 0.0],
                 ['14-00',  0.0, 0.0],
                 ['17-00',  0.0, 0.0]
               ])
    send_event("crateschart_1_#{phc}",
               points: [
                 %w[Packed Crates Short],
                 ['This Shift', 0.0, 0.0]
               ])
  end
end
