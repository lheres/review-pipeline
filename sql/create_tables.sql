CREATE TABLE review_facts (
   ts_review timestamp,
   reviewer_id varchar,
   reviewer_name varchar,
   time_review date,
   rating real,
   summary varchar,
   review_text varchar,
   helpful json
);

CREATE UNIQUE INDEX ON review_facts (
       rating,
       md5(summary),
       md5(review_text),
       time_review
);

CREATE TABLE metadata_dim (
	id primary,
	price double,
	asin varchar,
	title varchar,
	imUrl varchar,
	also_bought array,
	bought_together array,
	buy_after_viewing array,
	categories array,
	salesrank json
);



CREATE TABLE price_dim
	id primary,






CREATE TABLE reviewer_dim (
   id : primary
   reviewer_id : varchar,
   reviewer_name: varchar
)
   
   