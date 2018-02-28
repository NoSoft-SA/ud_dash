# frozen_string_literal: true

query = <<~HTML
  SELECT resources.resource_code AS packhouse_code, shift_types.day_night_or_custom,
  COALESCE(SUM(shifts.running_hours_at_10), 0.0) AS at_10,
  COALESCE(SUM(shifts.running_hours_at_14), 0.0) AS at_14,
  COALESCE(SUM(shifts.running_hours_at_17), 0.0) AS at_17,
  COALESCE(SUM(shifts.running_hours_at_21), 0.0) AS at_21,
  COALESCE(SUM(shifts.running_hours_at_0020), 0.0) AS at_0020,
  COALESCE(SUM(shifts.running_hours_at_03), 0.0) AS at_03
  FROM shifts
  JOIN shift_types ON shift_types.id = shifts.shift_type_id
  JOIN resources ON resources.id = shift_types.packhouse_resource_id
  WHERE '2017-10-06 10:00' BETWEEN shifts.start_date_time AND shifts.end_date_time -- localtimestamp
  GROUP BY resources.resource_code, shift_types.day_night_or_custom
HTML

# :first_in sets how long it takes before the job is first run.
# In this case, it is run immediately
SCHEDULER.every '1m', first_in: 0 do # |job|
  res = DBUD[query].all

  res.each do |rec|
    phc = rec[:packhouse_code]
    next unless PACKHOUSES_TO_USE.include?(phc)
    case rec[:day_night_or_custom]
    when 0
      # send bar with day values
      send_event("runningchart_#{phc}",
                 points: [
                   %w[Time Hours],
                   ['10-00',  4.5], # rec[:at_10].to_f],
                   ['14-00',  6.5], # rec[:at_14].to_f],
                   ['17-00',  7.2], # rec[:at_17].to_f]
                 ])
    when 1
      # send bar with night values
      send_event("runningchart_#{phc}",
                 points: [
                   %w[Time Hours],
                   ['21-00',  rec[:at_21].to_f],
                   ['00-20',  rec[:at_0020].to_f],
                   ['03-00',  rec[:at_03].to_f]
                 ])
    else
      # custom
      send_event("runningchart_#{phc}",
                 points: [
                   %w[Time Hours],
                   ['10-00',  rec[:at_10].to_f],
                   ['14-00',  rec[:at_14].to_f],
                   ['17-00',  rec[:at_17].to_f],
                   ['21-00',  rec[:at_21].to_f],
                   ['00-20',  rec[:at_0020].to_f],
                   ['03-00',  rec[:at_03].to_f]
                 ])
    end
  end
end
