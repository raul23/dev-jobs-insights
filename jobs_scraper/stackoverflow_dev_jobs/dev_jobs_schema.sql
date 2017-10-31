-- Schema for the jobs scraper application

-- Job posts
CREATE TABLE job_posts (
        id			        text PRIMARY KEY NOT NULL,
		author		        text,
		link				text,
		location			text
);

-- Job perks such as salary and remote (work)
CREATE TABLE job_perks (
        perk_id             INTEGER PRIMARY KEY NOT NULL,
		job_id				text NOT NULL REFERENCES job_posts(id),
		name				text NOT NULL,
		value               text NOT NULL
);

-- Overview info about job such as company size, exp level
CREATE TABLE job_overview (
        item_id             INTEGER PRIMARY KEY NOT NULL,
		job_id				text NOT NULL REFERENCES job_posts(id),
		name				text NOT NULL,
		value               text NOT NULL
);