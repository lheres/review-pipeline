{{ config({
	"materialized" : "table",
	"post-hook" : "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_asin ON {{ this }} USING BTREE (asin);
					CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_also_bought ON {{ this }} USING BTREE (also_bought);"
	}) }}
SELECT raw_json ->> 'asin'::text AS asin,
    jsonb_array_elements(raw_json -> 'related' -> 'also_bought')::text as also_bought
FROM {{ ref('metadata_raw') }}