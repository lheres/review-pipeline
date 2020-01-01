{{ config({
	"materialized" : "table",
	"post-hook" : "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_asin ON {{ this }} USING BTREE (asin);
					CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_also_viewed ON {{ this }} USING BTREE (also_viewed);"
	}) }}
SELECT raw_json ->> 'asin'::text AS asin,
	jsonb_array_elements(raw_json -> 'related' -> 'also_viewed')::text as also_viewed
FROM {{ ref('metadata_raw') }}