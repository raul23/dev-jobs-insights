-- Datetimes have the following format: YYYY-MM-DD HH:MM:SS-HH:MM

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
		job_post_id			integer not null references entries(job_post_id),
		name				text not null,
		primary key(job_post_id, name)
);
