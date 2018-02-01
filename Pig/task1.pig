REGISTER /usr/yelp/pig/json-simple-1.1.1.jar;
REGISTER /usr/yelp/pig/elephant-bird-hadoop-compat-4.3.jar;
REGISTER /usr/yelp/pig/elephant-bird-pig-4.3.jar;


A = LOAD '/usr/yelp/business.json'
USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') As jsonMap;

b = FOREACH A GENERATE jsonMap#'city' AS city,
(double) jsonMap#'stars' AS stars,
(double) jsonMap#'review_count' AS review_count,
flatten(jsonMap#'categories') AS categories;

c= Group b BY (city,categories);
d = FOREACH c GENERATE FLATTEN(group) as (city,categories), AVG(b.stars) as avg_stars, AVG(b.review_count) as avg_review;

STORE d INTO '/usr/hadoop/csvoutput/ques1'
    using PigStorage('\t','-schema');

dump d;	