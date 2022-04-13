#' Indeed Scraper
#'
#'

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'sentiment-analysis-indeed-scrape'
EF_DIR = Sys.getenv('EF_DIR')
RESET_SQL = FALSE

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

page_html =
	GET('https://www.indeed.com/jobs?l=31904&fromage=1&radius=25') %>%
	content(.)

zip_scrape = purrr::reduce(1:1000, .init = tibble(), .f = function(accum, page_number) {
	
	message(str_glue('Scraping page {page_number}'))
	
	page_html = content(
		GET(str_glue('https://www.indeed.com/jobs?l=31904&fromage=1&radius=25&start={(page_number - 1) * 10}')),
		encoding = 'UTF-8'
		)
	
	avail_pages = page_html %>% html_nodes('ul.pagination-list > li') %>% html_text(.) %>% keep(., ~ . != '')
	this_page = page_html %>% html_node('ul.pagination-list > li > b') %>% html_text(.)
	
	message(str_glue('Available page {paste0(avail_pages, collapse = ", ")}'))
	
	page_data =
		page_html %>%
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
		imap_dfr(., function(x, i) {
			# message(i)
			job_embed =
				GET(str_glue('https://www.indeed.com/viewjob?viewtype=embedded&jk={x$job_id}')) %>%
				content(.) %>%
				html_node(., 'div.jobsearch-JobComponent')
			
			job_header = tibble(
				scrape_target = c('job_title', 'company_name', 'company_location'),
				scrape_value = c(
					# https://stackoverflow.com/questions/56484967/scrape-first-class-node-but-not-child-using-rvest
					job_embed %>%
						html_node(., xpath = paste(selectr::css_to_xpath('h1.jobsearch-JobInfoHeader-title'), '/text()')) %>%
						html_text(.),
					job_embed %>%
						html_nodes(., 'div.jobsearch-InlineCompanyRating > div') %>%
						.[2] %>%
						html_text(.),
					job_embed %>%
						html_node(., 'div.jobsearch-InlineCompanyRating + div') %>%
						html_text(.)
					)
				)
			
			job_details =
				job_embed %>%
				html_nodes(., 'div.jobsearch-JobDescriptionSection-sectionItem') %>%
				map_dfr(., function(z)
					tibble(
						scrape_target =
							html_node(z, 'div.jobsearch-JobDescriptionSection-sectionItemKey') %>%
							html_text(.),
						scrape_value =
							html_nodes(z, '*:not(div.jobsearch-JobDescriptionSection-sectionItemKey)') %>%
							html_text(.) %>%
							paste0(., collapse = '\n')
					)
				) %>%
				{
					if (!is.null(.) && nrow(.) > 0) 
						mutate(
							.,
							scrape_target = case_when(
								str_detect(scrape_target, 'Job Type') ~ 'job_type',
								str_detect(scrape_target, 'Salary') ~ 'salary',
								TRUE ~ 'other_details'
							)
						)
					else tibble()
				}
			
	
			job_quals =
				job_embed %>%
				html_nodes(., 'li.jobsearch-ReqAndQualSection-item') %>%
				{
					if (length(.) == 0) tibble()
					else 
						tibble(
							scrape_target = 'job_quals',
							scrape_value = 
								html_text(.) %>% 
								paste0(., collapse = '\n')
						)
				}
			
			job_description =
				job_embed %>%
				html_nodes(., '#jobDescriptionText') %>%
				html_children(.) %>%
				{
					if (length(.) == 0) tibble()
					else 
						tibble(
							scrape_target = 'job_description',
							scrape_value = 
								html_text(.) %>%
								paste0(., collapse = '') %>%
								str_trim(.)
						)
				}
			
			job_activity =
				job_embed %>%
				html_nodes(., '.jobsearch-HiringInsights-entry--bullet') %>%
				html_text(.) %>%
				tibble(scrape_value = .) %>%
				{
					if (!is.null(.) && nrow(.) > 0) 
						mutate(
							.,
							scrape_target = case_when(
								str_detect(scrape_value, 'Posted') ~ 'posted_time',
								str_detect(scrape_value, 'Employer') ~ 'employer_id',
								TRUE ~ 'other_activity'
								),
							scrape_value = case_when(
								str_detect(scrape_value, 'Posted') ~ str_replace(scrape_value, 'Posted ', ''),
								str_detect(scrape_value, 'Employer') ~ str_replace(scrape_value, 'Employer reviewed job ', ''),
								TRUE ~ scrape_value
							)
						)
					else tibble()
				}
	
			res = 
				bind_rows(
					accum,
					job_details,
					job_header,
					job_quals,
					job_description,
					job_activity
				) %>%
				mutate(., indeed_id = x$job_id, scrape_page_number = page_number)
		})
	
	if (this_page == tail(avail_pages, 1)) return(done(page_data))
	
	return(page_data)
})