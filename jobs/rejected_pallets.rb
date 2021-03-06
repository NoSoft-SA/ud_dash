# frozen_string_literal: true

# This job returns pallets passed and rejected for the current ISO week.
# The data is displayed in a Meter widget and a list widget.
# The query returns a line per packhouse. Set the packhouses of interest in
# the constant "PACKHOUSES_TO_USE".

# FOR TESTING...
condition = ENV['TEST_DASH'] ? '28' : 'extract(week FROM current_date)'

query = <<~SQL
  SELECT packhouse_code, SUM(pallet_count) AS pallets_packed, SUM(passed_count) AS passed,
  SUM(rejected_count) AS rejected, SUM(not_inspected_count) AS not_inspected,
  (SUM(rejected_count)::numeric / (SUM(passed_count)::numeric + SUM(rejected_count)::numeric + SUM(not_inspected_count)::numeric) * 100.00)::integer AS percent_rejected
  FROM (
  SELECT DISTINCT ON (COALESCE(pallet_sequences.pallet_number, pallet_sequences.scrapped_pallet_number))
      1 AS pallet_count,
      CASE pallets.qc_result_status WHEN 'PASSED' THEN 1 ELSE 0 END AS passed_count,
      CASE pallets.qc_result_status WHEN 'FAILED' THEN 1 ELSE 0 END AS rejected_count,
      CASE pallets.ppecb_inspection_id IS NULL WHEN true THEN 1 ELSE 0 END AS not_inspected_count,
      resources.resource_code AS packhouse_code
     FROM pallets
       JOIN pallet_sequences ON COALESCE(pallet_sequences.pallet_number, pallet_sequences.scrapped_pallet_number) = pallets.pallet_number
       JOIN production_runs ON production_runs.id = pallet_sequences.production_run_id
       JOIN resources ON resources.id = production_runs.packhouse_resource_id
    JOIN fg_products ON fg_products.id = pallet_sequences.fg_product_id
    WHERE extract(week FROM pallet_sequences.packed_date_time) = #{condition}
    AND pallets.build_status = 'FULL'
    ORDER BY COALESCE(pallet_sequences.pallet_number, pallet_sequences.scrapped_pallet_number), pallet_sequences.pallet_sequence_number
    ) sub
  GROUP BY packhouse_code
  ORDER BY packhouse_code;
SQL

empty_list = [
  { label: 'Pallets Packed', value: 0 },
  { label: 'Passed', value: 0 },
  { label: 'Rejected', value: 0 },
  { label: 'Not Inspected', value: 0 }
].freeze
# :first_in sets how long it takes before the job is first run.
# In this case, it is run immediately
SCHEDULER.every '1m', first_in: 0 do # |job|
  res = DBUD[query].all

  ph_used = []
  res.each do |rec|
    phc = rec[:packhouse_code]
    next unless PACKHOUSES_TO_USE.include?(phc)
    ph_used << phc
    lines = rec.reject { |k, _| k == :packhouse_code || k == :percent_rejected }.map do |k, v|
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
    send_event("rejectchart_#{phc}",
               slices: [
                 ['Inspection',    'No Pallets'],
                 ['Passed',        rec[:passed]],
                 ['Rejected',      rec[:rejected]],
                 ['Not Inspected', rec[:not_inspected]]
               ])
  end

  # Make sure the dashboard is updated with zero-values
  # if there is no data for a packhouse:
  (PACKHOUSES_TO_USE - ph_used).each do |phc|
    send_event("rejectcounts_#{phc}", items: empty_list)
    send_event("rejectmin_#{phc}", value: 0)
    send_event("rejectchart_#{phc}",
               slices: [
                 ['Inspection',    'No Pallets'],
                 ['Passed',        0],
                 ['Rejected',      0],
                 ['Not Inspected', 0]
               ])
  end
end
