-- 1. Schema for the rss reader script
-- All dates have the following format: YYYY-MM-DD HH:MM:SS-HH:MM
-- TODO: all id (including job_id) should be integer not text
-- TODO: date (like updated, published) should be in the DATE data type, not text
-- TODO: 'job_overview' and 'job_perks' should present the data for the same 'job_id'
-- on the same row

-- Feeds
CREATE TABLE feeds (
		name		        text PRIMARY KEY NOT NULL,
		title		        text,
		updated		        text
);

-- Entries
-- TODO: rename `id` to `job_id`
CREATE TABLE entries (
		id			        text PRIMARY KEY NOT NULL,
		feed_name		    text NOT NULL,
		title 			    text,
		author              text,
		link				text,
		summary             text,
		published		    text,
		FOREIGN KEY(feed_name) REFERENCES feeds(name)
);

-- Tags
CREATE TABLE tags (
		name				text PRIMARY KEY NOT NULL
);

-- Association table between entries and tags
-- TODO: rename `id` to `job_id`
CREATE TABLE entries_tags (
		id					text NOT NULL REFERENCES entry(id),
		name				text NOT NULL REFERENCES tag(name),
		PRIMARY KEY(id, name)
);


-- 2. Schema for the jobs scraper script
-- Job posts
-- TODO: rename `id` to `job_id`
-- TODO: check in data_loading.py, `location` can have two locations in one separated by ;
-- e.g. Teunz, Germany; Kastl, Germany
CREATE TABLE job_posts (
        id			        text PRIMARY KEY NOT NULL,
		author		        text,
		link				text,
		location			text,

);

-- Job perks such as salary and remote (work)
CREATE TABLE job_perks (
        perk_id             INTEGER PRIMARY KEY NOT NULL,
		job_id				text NOT NULL REFERENCES job_posts(id),
		name				text NOT NULL, -- perk name
		value               text NOT NULL -- perk value
);

-- Overview info about job such as company size, exp level
CREATE TABLE job_overview (
        item_id             INTEGER PRIMARY KEY NOT NULL,
		job_id				text NOT NULL REFERENCES job_posts(id),
		name				text NOT NULL, -- name of info
		value               text NOT NULL -- value of info
);

-- Job salary
-- TODO: test this CREATE statement by executing it. I only executed it on the terminal
-- TODO: it is better to add min and max salaries on the same row, i.e. remove the column
-- 'name' and add the columns 'currency' (text), 'min_salary' (float), and 'max_salary' (float)
-- NOTE: name can be for any salary in any currencies that's why we can't have
-- 'min salary (USD)' as column name because we want flexibility
CREATE TABLE job_salary (
        id                  INTEGER PRIMARY KEY NOT NULL,
		job_id				text NOT NULL REFERENCES job_posts(id),
		name		        text NOT NULL, -- name of salary (e.g. 'max salary (USD)' or 'salary (CAD)')
		value               FLOAT NOT NULL -- value of salary
);