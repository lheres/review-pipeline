SELECT raw_json ->> 'asin'::text AS asin,
	jsonb_array_elements(raw_json -> 'related' -> 'also_viewed')::text as also_viewed
FROM {{ ref('metadata_raw') }}