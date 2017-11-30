# Table of Contents
<!-- TOC depthFrom:2 depthTo:6 withLinks:1 updateOnSave:1 orderedList:0 -->

- [Project description](#project-description)
- [Results](#results)
	- [Graphs](#graphs)
		- [Graphs: Tags/Technologies](#graphs-tagstechnologies)
			- [Bar chart: 20 most popular technologies](#bar-chart-20-most-popular-technologies)
		- [Graphs: Salary](#graphs-salary)
			- [Histogram: Mid-range salaries](#histogram-mid-range-salaries)
			- [Scatter plot: Average mid-range salary of Industries](#scatter-plot-average-mid-range-salary-of-industries)
			- [Scatter plot: Average mid-range salary of Countries](#scatter-plot-average-mid-range-salary-of-countries)
			- [Scatter plot: Average mid-range salary of US states](#scatter-plot-average-mid-range-salary-of-us-states)
			- [Scatter plot: Average mid-range salary of Job Roles](#scatter-plot-average-mid-range-salary-of-job-roles)
			- [Scatter plot: Average mid-range salary of Technologies](#scatter-plot-average-mid-range-salary-of-technologies)
		- [Graphs: Industries](#graphs-industries)
			- [Bar chart: 20 most popular industries](#bar-chart-20-most-popular-industries)
		- [Graphs: Locations](#graphs-locations)
			- [Bar chart: 20 most popular countries](#bar-chart-20-most-popular-countries)
			- [Bar chart: US states popularity](#bar-chart-us-states-popularity)
- [Conclusion](#conclusion)
	- [Future improvements](#future-improvements)

<!-- /TOC -->

## Project description
**dev_jobs_insights** is a *personal* data mining project written in *Python* with the
main objective of extracting meaningful insights from developer job posts. These
insights can help us in getting a more accurate picture of what the developer job
market looks like so we can take better decisions (e.g. what technology to focus
on).

Data visualization is an important tool in interpreting data. Thus graphs and maps
are an important aspect of this project as you can see from the multiple graphs
generated so we can see the developer job market along different dimensions (e.g.
salary, industries).

The project **source code** written in Python 3 (Python 2.7 will eventually be
supported) is found at [https://github.com/dev_jobs_insights](https://github.com/dev_jobs_insights). The code is
commented but there is not yet documentation on how to install and run the code
to generate the graphs. The documentation will be soon made available in case you
want to reproduce the [results](#results) presented below.

## Results
The job posts data was mined from StackOverflow developer jobs [RSS feed](https://stackoverflow.com/jobs/feed)
and [website](https://stackoverflow.com/jobs). The following table presents important
information about the extracted data from the job posts:

| NAME | VALUE | NOTES |
|:-------:|:-------:|:-------:|
| Sources of data | StackOverflow [RSS feed](https://stackoverflow.com/jobs/feed) and [dev jobs website](https://stackoverflow.com/jobs) |  |
| Published date | from 2017-09-26 to 2017-10-26 |  |
| Number of job posts | 933 |  |
| Number of job posts with salary info | 301 |  |
| Number of companies | 524 |  |
| Number of countries | 46 | Complete list |
| Number of US states | 22 | Complete list |
| Number of tags/technologies | 651 | e.g. Java, Python, angularjs |
| Number of Industries | 258 | e.g. Information Technology, eCommerce, Big Data |
| Number of Job roles | 14 | e.g. Backend Developer, Mobile Developer |


**NOTE**:
- the number of job posts is small but I intend on integrating more data from
StackOverflow and other sources of job posts data

### Graphs

#### Graphs: Tags/Technologies
##### Bar chart: 20 most popular technologies

#### Graphs: Salary
**DISCLAIMER:**  since ...
##### Histogram: Mid-range salaries
##### Scatter plot: Average mid-range salary of Industries
##### Scatter plot: Average mid-range salary of Countries
##### Scatter plot: Average mid-range salary of US states
##### Scatter plot: Average mid-range salary of Job Roles
##### Scatter plot: Average mid-range salary of Technologies

#### Graphs: Industries
##### Bar chart: 20 most popular industries

#### Graphs: Locations
##### Bar chart: 20 most popular countries
##### Bar chart: US states popularity

## Conclusion

### Future improvements
List of **important** improvements:
- automate the whole *data mining/graphs generation* process so it can be run
weekly for example
- more sources of data: integrate more job posts from other job sites
- write documentation on how to install and run the project's code
- create a web interface to interact with the code: program configuration done
through a web dashboard for example instead of editing an INI configuration file like
it is done right now

**Non-urgent** improvements:
- add support for *python 2.7*
- add support for *multiprocessing* to speed up completion of tasks
- use *plotly* for generating all the graphs instead of also using *matplotlib*
