REGISTER /usr/yelp/pig/json-simple-1.1.1.jar;
REGISTER /usr/yelp/pig/elephant-bird-hadoop-compat-4.3.jar;
REGISTER /usr/yelp/pig/elephant-bird-pig-4.3.jar;

A	=	LOAD '/usr/yelp/business.json'
USING	com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad=true')	AS	(yelp);	

business	=	FOREACH	A	GENERATE	flatten(yelp#'categories') AS categories,
	yelp#'business_id'	as	business_id,
	yelp#'city'	as	city,
	(double) yelp#'stars' AS stars,
	(double) yelp#'review_count' AS review_count,
	(float)yelp#'latitude'	as	latitude,
	(float)yelp#'longitude'	as	longitude	;
	
b	= FILTER	business	BY	(((ACOS((SIN(43.6532*3.142/180)*SIN((latitude)*3.142/180))+(COS((latitude)*3.142/180)*COS(43.6532*3.142/180)*COS((79.3832*3.142*(-1)/180)-((longitude)*3.142/180)))))*6371)<15);
c = GROUP b BY categories;
d = FOREACH c GENERATE group as categories, AVG(b.stars) as avg_star,  AVG(b.review_count) as avg_review;

STORE d INTO '/usr/hadoop/csvoutput/ques4'
    using PigStorage('\t','-schema');
	
dump d;
