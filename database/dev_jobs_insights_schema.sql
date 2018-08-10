-- All dates have the following format: YYYY-MM-DD HH:MM:SS-HH:MM
-- TODO: 'job_overview' and 'job_perks' should present the data for the same 'job_id'
-- on the same row

-- Feeds
CREATE TABLE feeds (
        feed_id             integer primary key,
		name		        text not null,
		title		        text,
		updated		        datetime
);

-- Entries
-- `job_id` comes from the entry's id attribute found in the job rss feeds
create table entries (
		job_id			    integer primary key not null,
		feed_name		    text not null,
		title 			    text,
		author              text,
		link				text,
		summary             text,
		published		    datetime,
		foreign key(feed_name) references feeds(name)
);

-- Tags
create table tags (
		job_id				integer not null references entries(job_id),
		name				text not null,
		primary key(job_id, name)
);

-- Job posts
-- TODO: check in data_loading.py, `locations` can refer to two locations separated by ;
-- e.g. Teunz, Germany; Kastl, Germany
create table job_posts (
        job_id			    integer primary key not null,
		author		        text,
		link				text
);

create table location (
		job_id				integer not null references job_posts(job_id),
		name				text not null,
		primary key(job_id, name)
);

-- Job perks such as salary and remote (work)
CREATE TABLE job_perks (
        perk_id             integer primary key not null,
		job_id				text not null references  job_posts(job_id),
		name				text not null, -- perk name
		value               text not null -- perk value
);

-- Overview info about a job such as company size, exp level
CREATE TABLE job_overview (
        item_id             integer primary key not null,
		job_id				integer not null references job_posts(job_id),
		name				text not null, -- name of info
		value               text not null -- value of info
);

-- Job salary
-- TODO: test this CREATE statement by executing it. I only executed it on the terminal
CREATE TABLE job_salary (
        salary_id           integer primary key not null,
		job_id				text not null references job_posts(job_id),
		min_salary          float not null,
		max_salary          float not null,
		currency            text not null,
		value               float not null -- value of salary
);