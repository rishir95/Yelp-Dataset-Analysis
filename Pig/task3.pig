REGISTER /usr/yelp/pig/json-simple-1.1.1.jar;
REGISTER /usr/yelp/pig/elephant-bird-hadoop-compat-4.3.jar;
REGISTER /usr/yelp/pig/elephant-bird-pig-4.3.jar;


A = LOAD '/usr/yelp/business.json'
USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') As jsonMap;

b = FOREACH A GENERATE jsonMap#'city' AS city,
(double) jsonMap#'stars' AS stars,
flatten(jsonMap#'categories') AS categories,
flatten(jsonMap#'attributes'#'RestaurantsTakeOut') AS attributes;

c = FILTER b by categories == 'Mexican' AND attributes == 'true';

d = GROUP c BY (categories,attributes);
e = FOREACH d GENERATE FLATTEN(group) as (categories,attributes) , AVG(c.stars) as avg_star;

STORE e INTO '/usr/hadoop/csvoutput/task3'
    using PigStorage('\t','-schema');

dump e;
