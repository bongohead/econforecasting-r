#'  This pulls vintage dates for WSJ survey releases older than 4/1/2021 (when the survey was monthly).
#'  These are joined on recession data and the results are dumped in a CSV for use in external-import-wsj.

# Initialize ----------------------------------------------------------

## Load Libs ----------------------------------------------------------
library(econforecasting)
library(tidyverse)
library(httr2)
library(readxl, include.only = c('excel_sheets', 'read_excel'))
library(fs, include.only = c('file_temp'))

# Import ----------------------------------------------------------

## Get Vintage Dates ----------------------------------------------------------
#' Pull vintage dates by iterating through article dates
#' Drop data where no article exists
vintage_date_map =
	# This is a dump of the article HTML
	read_file(file.path(Sys.getenv('EF_DIR'), 'modules', 'external-import', 'wsj-backfill-raw-html.html')) %>%
	read_html() %>%
	html_nodes(., 'ul > li') %>%
	map(., .progress = T, function(x) {
		Sys.sleep(.2)
		links =
			html_nodes(x, 'a') %>%
			html_attr('href')

		if (length(links) != 2) {
			print('Err')
			return(NA)
		}

		date_el =
			request(links[[1]]) %>%
			add_standard_headers() %>%
			req_perform(.) %>%
			resp_body_html(.) %>%
			{
				if (length(html_nodes(., 'time')) > 0) html_node(., 'time')
				else
					html_nodes(., 'p') %>%
					keep(., \(x)
						 (!is.na(html_attr(x, 'class')) & str_detect(html_attr(x, 'class'), 'time|Time')) |
						 	str_sub(html_text2(x), -3) == ' ET' |
						 	str_sub(html_text(x), 1, 8) == 'Updated '
					) %>%
					.[[1]]
			} %>%
			html_text(.)

		date_2 = str_extract_all(date_el, '\\d{4}')[[1]]
		date_1 = str_extract(date_el, ".+?(?=\\d{4})") %>% str_replace_all(., 'Updated ', '')
		vdate =
			paste0(date_1, date_2) %>%
			str_replace_all(., 'Sept', 'Sep') %>%
			mdy(.)

		res = tibble(
			vdate = vdate,
			link = links[[2]]
		)

		print(res)

		res
	})

## Join to Recessions Input CSV ----------------------------------------------------------
vintage_date_maps =
	vintage_date_map %>%
	keep(., is_tibble) %>%
	list_rbind %>%
	mutate(., date = str_sub(fs::path_ext_remove(basename(link)), -4)) %>%
	mutate(., date = paste0(str_sub(date, 1, 2), '01', str_sub(date, 3, 4))) %>%
	mutate(., date = mdy(date)) %>%
	filter(., floor_date(vdate, 'month') == date)

res =
	read_csv(file.path(Sys.getenv('EF_DIR'), 'modules', 'external-import', 'wsj-backfill-raw-recess.csv')) %>%
	transmute(., date = mdy(date), value = as.numeric(value)) %>%
	left_join(., vintage_date_maps, by = 'date') %>%
	arrange(., desc(date)) %>%
	na.omit(.) %>%
	transmute(
		forecast = 'wsj',
		form = 'd1',
		freq = 'm',
		varname = 'recess',
		vdate,
		date,
		value
	)

## Dump Results ----------------------------------------------------------
write_csv(res, file.path(Sys.getenv('EF_DIR'), 'modules', 'external-import', 'wsj-backfill-cleaned-recess.csv'))
