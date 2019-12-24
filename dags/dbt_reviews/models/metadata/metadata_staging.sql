 SELECT raw_json ->> 'asin'::text AS asin,
    raw_json ->> 'title'::text AS title,
    raw_json ->> 'Brand'::text AS brand,
	(raw_json ->> 'price')::real AS price
 FROM {{ ref('metadata_raw') }}