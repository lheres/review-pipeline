SELECT 
	raw_json ->> 'asin' as asin,
	to_timestamp((raw_json ->> 'unixReviewTime')::bigint) as ts_review,
	raw_json ->> 'reviewerID' as reviewer_id,
	raw_json ->> 'reviewerName' as reviewer_name,
	(raw_json ->> 'reviewTime')::timestamp as time_review,
	raw_json ->> 'overall' AS rating,
	raw_json ->> 'helpful' AS helpful
FROM {{ ref('reviews_raw') }}
