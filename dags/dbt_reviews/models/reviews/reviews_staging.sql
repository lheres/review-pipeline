SELECT 
	review->>'asin' as asin,
	to_timestamp((review->>'unixReviewTime')::bigint) as ts_review,
	review->>'reviewerID' as reviewer_id,
	review->>'reviewerName' as reviewer_name,
	(review->>'reviewTime')::timestamp as time_review,
	review->>'overall' AS rating,
	review->>'helpful' AS helpful
FROM {{ ref('reviews_raw') }}
