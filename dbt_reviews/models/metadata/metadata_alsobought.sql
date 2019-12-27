SELECT raw_json ->> 'asin'::text AS asin,
    jsonb_array_elements(raw_json -> 'related' -> 'also_bought')::text as also_bought
FROM {{ ref('metadata_raw') }}