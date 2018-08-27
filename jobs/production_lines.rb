query = <<~SQL
  SELECT production_runs.line_code, production_runs.production_run_status,
    production_runs.production_run_number, production_runs.farm_code,
    production_runs.production_run_code, production_runs.production_run_stage,
    production_runs.batch_code, product_classes.product_class_code,
    track_indicators.track_indicator_code, treatments.treatment_code, rank, sizes.size_code,
    stats.bins_tipped, stats.bins_tipped_weight, stats.cartons_printed, stats.cartons_weight,
    CAST(stats.cartons_weight / COALESCE(stats.bins_tipped_weight, 1) * 100 AS numeric(3)) AS percent_packed, stats.pallets_completed
  FROM production_runs
  LEFT JOIN product_classes on product_classes.id = production_runs.product_class_id
  LEFT JOIN treatments ON treatments.id = production_runs.treatment_id
  LEFT JOIN track_indicators ON track_indicators.id = production_runs.track_indicator_id
  LEFT JOIN sizes ON sizes.id = production_runs.size_id
  LEFT JOIN production_run_stats stats ON stats.production_run_id = production_runs.id
  WHERE production_runs.line_code = '41' AND (production_runs.production_run_status = 'active' OR production_runs.production_run_status = 'reconfiguring')
    AND production_runs.id IS NOT null
    AND (production_run_stage = 'bintipping_only' OR production_run_stage = 'bintipping_plus' OR production_run_stage = 'carton_labeling_plus')
SQL

LINES_TO_USE = ['41', '42']

SCHEDULER.every '1m', :first_in => 0 do
  res = DBKR[query].all # .map { |r| { label: r[:status], value: r[:no_bins] } }
  # lkp = {}
  headers = { production_run_code: 'Run',
              farm_code: 'Farm',
              production_run_stage: 'Stage',
              production_run_status: 'Status',
              production_run_number: 'Number',
              batch_code: 'Batch',
              product_class_code: 'Class',
              track_indicator_code: 'TI',
              treatment_code: 'Treatment',
              rank: 'Rank',
              size_code: 'Size'}

  # res.each do |rec|
  #
  #   lc = rec[:line_code]
  #   lkp[lc] ||= []
  #   %w{production_run_code line_phc farm_code puc_code}.each do |key|
  #     sym = key.to_sym
  #     hs = { label: headers[sym], value: rec[sym] }
  #     lkp[lc] << hs unless lkp[lc].include?(hs)
  #   end
  # end

  res.each do |rec|
    ln = rec[:line_code]
    next unless LINES_TO_USE.include?(ln) # NOTE: this could be ignored - all lines not THAT much data...
    # send_event("line_text_#{ln}", { title: rec[:production_run_code], moreinfo: "<< LINE #{ln} >>" })
    send_event("line_packed_#{ln}", { value: rec[:percent_packed] })
    send_event("line_cartons_#{ln}", { current: rec[:cartons_printed] })
    send_event("line_palletizing_#{ln}", { current: rec[:pallets_completed] })
    send_event("line_bins_#{ln}", { items: [{ label: 'Count', value: rec[:bins_tipped] }, { label: 'Weight', value: sprintf('%.2f kg', rec[:bins_tipped_weight] || 0.0) }] })
    items = []
    [:production_run_code,
     :farm_code,
     :product_class_code,
     :size_code,
     :track_indicator_code,
     :batch_code,
     :production_run_stage,
     :production_run_status,
     :production_run_number,
     :treatment_code,
     :rank].each do |key|
      items << {label: headers[key], value: rec[key] }
    end
    send_event("line_details_#{ln}", { items: items })
  end
end
