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
        job_id			    integer primary key not null references entries(job_id),
        author				text,
		link				text,
		title               text,
		hiring_organization text,
		employment_type     text,
        date_posted          date,
		valid_through        date
);

create table exp_level (
		job_id				integer not null references job_posts(job_id),
		level			    text not null,
		primary key(job_id, exp_level)
);

create table industry (
		job_id				integer not null references job_posts(job_id),
		name			    text not null,
		primary key(job_id, name)
);


-- TODO: check if skills = tags? If yer, then this table might be redundant
create table skills (
		job_id				integer not null references job_posts(job_id),
		skill				text not null,
		primary key(job_id, skill)
);

create table job_benefits (
		job_id				integer not null references job_posts(job_id),
		name				text not null,
		primary key(job_id, name)
);

CREATE TABLE job_salary (
		job_id				integer primary key not null references job_posts(job_id),
		min_value           integer not null,
		max_value           integer not null,
		currency            text not null
);

-- TODO: check if only one location per job post
create table location (
        loc_id              integer primary key,
		job_id				integer not null references job_posts(job_id),
		country				text not null,
		city                text not null
);