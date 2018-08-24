-- All dates have the following format: YYYY-MM-DD HH:MM:SS-HH:MM
-- TODO: 'job_overview' and 'job_perks' should present the data for the same 'job_id'
-- on the same row

-- Feeds
create table feeds (
        feed_id             integer primary key not null,
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
		author              text,  -- company name
		url				    text,
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
create table job_posts (
        job_id			        integer primary key not null references entries(job_id),
		title                   text,
		url 				    text, -- job post's URL
		company_name		    text,
		employment_type         text, -- e.g. FULL_TIME
		job_post_description    text,
		job_post_terminated     boolean, -- True if job post not accepting job applications anymore
		equity                  boolean, -- True if there is equity involved
		remote                  text,
		relocation              text,
		visa                    text,
		cached_webpage_path     text, -- file path of job post's cached webpage
        date_posted             date,  -- YYYY-MM-DD
		valid_through           date,  -- YYYY-MM-DD
		webpage_accessed        datetime -- YYYY-MM-DD HH:MM:SS-HH:MM
);

create table hiring_company (
        job_id				integer primary key not null references entries(job_id),
        name                text, -- company name
        url                 text, -- company site URL
        description         text, -- company description
        type                text, -- company type, e.g. VC or private
        size                integer, -- company size (number of people)
        high_response_rate  boolean -- True if company has high response rate
);

create table experience_level (
		job_id				integer not null references entries(job_id),
		level			    text not null, -- experience level, e.g. senior, junior
		primary key(job_id, level)
);

create table role (
		job_id				integer not null references entries(job_id),
		name			    text not null, -- name of the job role, e.g. Manager
		primary key(job_id, name)
);

create table industry (
		job_id				integer not null references entries(job_id),
		name    			text not null, -- e.g Entertainment
		primary key(job_id, name)
);


-- TODO: check if skills = tags = technologies? If yes, then this table might be redundant
create table skills (
		job_id				integer not null references entries(job_id),
		skill				text not null,
		primary key(job_id, skill)
);

create table job_benefits (
		job_id				integer not null references entries(job_id),
		name				text not null,
		primary key(job_id, name)
);

create table job_salary (
        job_salary_id       integer primary key,
		job_id				integer not null,
		min_salary          integer not null,
		max_salary          integer not null,
		currency            text not null,
		conversion_time     datetime, -- currency conversion time YYYY-MM-DD HH:MM:SS-HH:MM
		foreign key(job_id) references entries(job_id)
);

create table job_location (
        loc_id              integer primary key,  -- auto-increment by SQLite
		job_id				integer not null,
		city                text,
		region              text, -- region = province, e.g. Ontario
		country				text not null,
		foreign key(job_id) references entries(job_id)
);