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
	(float)yelp#'longitude'	as	longitude,
	yelp#'name' as name;
	
b	= FILTER	business	BY	(((ACOS((SIN(43.6532*3.142/180)*SIN((latitude)*3.142/180))+(COS((latitude)*3.142/180)*COS(43.6532*3.142/180)*COS((79.3832*3.142*(-1)/180)-((longitude)*3.142/180)))))*6371)<15);
c = FILTER b BY (categories == 'Food');
d = GROUP c by name;
e = FOREACH d GENERATE group as name , AVG(c.stars) as star, FLATTEN(c.business_id) as business_id;
f1 = ORDER e BY star DESC;
f2 = ORDER e BY star ASC;
g1 = LIMIT f1 10;
g2 = LIMIT f2 10;
g = UNION g1, g2;

B =	LOAD '/usr/review.json' 
USING	com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad=true	')	AS	(review);

revie =	FOREACH	B GENERATE review#'business_id' as	business_id,
	(double)review#'stars'	as stars,
	review#'date' as date;

datajoin = JOIN g BY business_id, revie BY business_id;
datajoin1 = FOREACH datajoin GENERATE name, date, stars;
datefilter = FILTER datajoin1 BY (date matches '.*-01-.*') OR (date matches '.*-02-.*') OR (date matches '.*-03-.*') OR (date matches '.*-04-.*') OR (date matches '.*-05-.*');
grp = GROUP datefilter BY name;
finalset = FOREACH grp GENERATE group as name, AVG(datefilter.stars) as avg_star;
STORE finalset INTO '/usr/hadoop/csvoutput/ques5'
    using PigStorage('\t','-schema');

dump finalset;