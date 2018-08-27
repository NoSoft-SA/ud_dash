# frozen_string_literal: true

# This job returns pallets passed and rejected for the current ISO week.
# The data is displayed in a Meter widget and a list widget.
# The query returns a line per packhouse. Set the packhouses of interest in
# the constant "PACKHOUSES_TO_USE".

# Counts: Pallets
# not inspe
# not %
# exclude half-pallets (build_status)
query = <<~SQL
  SELECT packhouse_code, SUM(pallet_count) AS pallets_packed, SUM(passed_count) AS passed, SUM(rejected_count) AS rejected,
  (SUM(rejected_count)::numeric / (SUM(passed_count)::numeric + SUM(rejected_count)::numeric) * 100.00)::integer AS percent_rejected
  FROM (
  SELECT distinct on (coalesce(pallet_sequences.pallet_number, pallet_sequences.scrapped_pallet_number))
    1 AS pallet_count, resources.resource_code AS packhouse_code,
    CASE ppecb_inspections.passed WHEN true THEN 1 ELSE 0 END AS passed_count,
    CASE ppecb_inspections.passed WHEN true THEN 0 ELSE 1 END AS rejected_count
    -- not-inspected
  FROM pallet_sequences
  LEFT OUTER JOIN pallets ON pallets.id = pallet_sequences.pallet_id
  LEFT OUTER JOIN ppecb_inspections ON ppecb_inspections.id = pallets.ppecb_inspection_id
  JOIN production_runs ON production_runs.id = pallet_sequences.production_run_id
  JOIN resources ON resources.id = production_runs.packhouse_resource_id
  WHERE extract(week FROM pallet_sequences.packed_date_time) = 30 -- extract(week FROM current_date)
  ) sub
  GROUP BY packhouse_code
SQL

# :first_in sets how long it takes before the job is first run.
# In this case, it is run immediately
SCHEDULER.every '1m', first_in: 0 do # |job|
  res = DBUD[query].all

  res.each do |rec|
    phc = rec[:packhouse_code]
    next unless PACKHOUSES_TO_USE.include?(phc)
    lines = rec.reject { |k, _| k == :packhouse_code }.map do |k, v|
      {
        label: k.to_s
                .split('_')
                .map(&:capitalize)
                .join(' ')
                .sub('Percent', '%'),
        value: v.is_a?(Integer) ? v : format('%.2f', v)
      }
    end
    send_event("rejectcounts_#{phc}", items: lines)
    send_event("rejectmin_#{phc}", value: rec[:percent_rejected])
    send_event("rejectchart_#{phc}", slices: [
                ['Inspection', 'No Pallets'],
                ['Passed',     rec[:passed]],
                ['Rejected',   rec[:rejected]]
              ])
  end
end
