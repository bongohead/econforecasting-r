#' Indeed Scraper
#'
#'

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'sentiment-analysis-indeed-scrape'
EF_DIR = Sys.getenv('EF_DIR')
RESET_SQL = FALSE
BACKFILL_REDDIT = TRUE
BACKFILL_REUTERS = TRUE

## Cron Log ----------------------------------------------------------
if (interactive() == FALSE && rstudioapi::isAvailable(child_ok = T) == F) {
	sink_path = file.path(EF_DIR, 'logs', paste0(JOB_NAME, '.log'))
	sink_conn = file(sink_path, open = 'at')
	system(paste0('echo "$(tail -50 ', sink_path, ')" > ', sink_path,''))
	lapply(c('output', 'message'), function(x) sink(sink_conn, append = T, type = x))
	message(paste0('\n\n----------- START ', format(Sys.time(), '%m/%d/%Y %I:%M %p ----------\n')))
}

## Load Libs ----------------------------------------------------------'
library(econforecasting)
library(tidyverse)
library(data.table)
library(httr)
library(rvest)
library(RCurl)
library(DBI)
library(RPostgres)
library(lubridate)
library(jsonlite)

## Load Connection Info ----------------------------------------------------------
source(file.path(EF_DIR, 'model-inputs', 'constants.r'))
db = dbConnect(
	RPostgres::Postgres(),
	dbname = CONST$DB_DATABASE,
	host = CONST$DB_SERVER,
	port = 5432,
	user = CONST$DB_USERNAME,
	password = CONST$DB_PASSWORD
)

# Scrape ----------------------------------------------------------
## Scrape ----------------------------------------------------------

page_data =
	GET('https://www.indeed.com/jobs?l=31904') %>%
	content(.) %>%
	html_node(., '#mosaic-zone-jobcards')

page_info =
	page_data %>%
	html_nodes('a.tapItem') %>%
	map_dfr(., function(x) tibble(
		job_title = x %>% html_node(., 'h2.jobTitle > span') %>% html_text(.),
		job_id = x %>% html_attr(., 'data-jk'),
		company_name = x %>% html_node(., 'span.companyName') %>% html_text(.),
		company_location = x %>% html_node(., 'div.companyLocation') %>% html_text(.),
		job_snippet = x %>% html_node(., 'div.job-snippet') %>% html_text(.)
		))

page_data =
	page_info %>%
	purrr::transpose(.) %>%
	.[1:1] %>%
	lapply(., function(x) {
		job_embed =
			GET(str_glue('https://www.indeed.com/viewjob?viewtype=embedded&jk={x$job_id}')) %>%
			content(.) %>%
			html_node(., 'div.jobsearch-JobComponent')
		
		tibble(
			job_title =
				# https://stackoverflow.com/questions/56484967/scrape-first-class-node-but-not-child-using-rvest
				job_embed %>%
				html_node(., xpath = paste(selectr::css_to_xpath('h1.jobsearch-JobInfoHeader-title'), '/text()')) %>%
				html_text(.)
		)
	})