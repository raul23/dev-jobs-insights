-- Datetimes have the following format: YYYY-MM-DD HH:MM:SS-HH:MM
-- Dates have the following format: YYYY-MM-DD

-- RSS Feeds
create table feeds (
        feed_id             integer primary key not null,
		name		        text not null,
		title		        text,
		updated		        datetime
);

-- Entries
-- `job_post_id` comes from the entry's id attribute found in the RSS feeds
create table entries (
		job_post_id			integer primary key not null,
		feed_name		    text not null,
		title 			    text,
		author              text,  -- company name
		url				    text,
		location            text,
		summary             text,
		published		    datetime,
		foreign key(feed_name) references feeds(name)
);

-- Tags
create table tags (
		job_post_id			integer not null references entries(job_id),
		name				text not null,
		primary key(job_id, name)
);

create table companies (
        id				    integer primary key not null,
        name                text not null, -- company name
        url                 text, -- company site URL
        description         text, -- company description
        company_type        text, -- e.g. VC or private
        company_min_size    integer, -- minimum number of people working
        company_max_size    integer, -- maximum number of people working
        high_response_rate  boolean default(false) -- True if company has high response rate
);

-- Job posts
create table job_posts (
        id			            integer primary key not null references entries(job_post_id),
        company_id              integer not null,
		title                   text not null,
		url 				    text not null, -- job post's URL
		employment_type         text, -- e.g. FULL_TIME
		job_post_description    text not null,
		job_post_terminated     boolean default(false), -- True if job post not accepting job applications anymore
		job_post_removed        boolean default(false), -- True if job post was removed
		equity                  boolean default(false), -- True if there is equity involved
		remote                  text,
		relocation              text,
		visa                    text,
		cached_webpage_filepath text, -- file path of job post's cached webpage
        date_posted             date,
		valid_through           date,
		webpage_accessed        datetime,
		foreign key(company_id) references companies(id)
);

create table experience_levels (
		job_post_id			integer not null,
		name			    text not null, -- experience level, e.g. senior, junior
		primary key(job_post_id, level),
		foreign key(job_post_id) references job_posts(job_post_id)
);

create table industries (
		job_post_id			integer not null,
		name    			text not null, -- e.g Entertainment
		primary key(job_post_id, name),
		foreign key(job_post_id) references job_posts(job_post_id)
);

create table job_benefits (
		job_post_id			integer not null,
		name				text not null,
		primary key(job_post_id, name),
		foreign key(job_post_id) references job_posts(job_post_id)
);

create table job_locations (
        id                  integer primary key not null,  -- auto-increment by SQLite
		job_id				integer not null,
		city                text,
		region              text, -- region = province, e.g. Ontario
		country				text not null,
		foreign key(job_post_id) references job_posts(job_post_id)
);

create table job_salaries (
        id                  integer primary key not null,
		job_id				integer not null,
		min_salary          integer not null,
		max_salary          integer not null,
		currency            text not null,
		conversion_time     datetime, -- currency conversion time YYYY-MM-DD HH:MM:SS-HH:MM
		foreign key(job_post_id) references job_posts(job_post_id)
);

create table roles (
		job_post_id			integer not null,
		name			    text not null, -- name of the job role, e.g. Manager
		primary key(job_post_id, name),
		foreign key(job_post_id) references job_posts(job_post_id)
);

-- TODO: check if skills = tags = technologies? If yes, then this table might be redundant
create table skills (
		job_post_id			integer not null,
		name				text not null,
		primary key(job_post_id, skill),
		foreign key(job_post_id) references job_posts(job_post_id)
);