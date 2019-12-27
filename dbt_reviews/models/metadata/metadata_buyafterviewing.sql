SELECT raw_json ->> 'asin'::text AS asin,
	jsonb_array_elements(raw_json -> 'related' -> 'buy_after_viewing')::text as buy_after_viewing
FROM {{ ref('metadata_raw') }}