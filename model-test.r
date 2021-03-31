

degreesList =
	list(
		'biology', 'chemistry', 'physics', 'psychology',
		'mathematics', 'statistics', 'computer science',
		'economics', 'political science', 'sociology',
		'finance', 'accounting', 'marketing',
		'english', 'philosophy', 'linguistics', 'literature', 'history', 'geography',
		'communications', 'journalism',
		'forestry', 'environmental science', 'agriculture',
		'architecture',
		'women\'s studies', 
		'fine arts', 'dance', 'graphic design', 'music', 'photography',
		'chemical engineering', 'civil engineering', 'nuclear engineering', 'mechanical engineering', 'electrical engineering'
		)

df =
	lapply(degreesList, function(degree) {
		message(degree)
		httr::GET(paste0('https://www.indeed.com/jobs?q=', URLencode(degree), '+degree&sort=date&start=10')) %>%
			httr::content(.) %>%
			html_nodes(., '#searchCountPages') %>%
			html_text(.) %>%
			str_split_fixed(., ' of ', 2) %>% 
			.[1, 2] %>%
			gsub('[^0-9.-]', '', .) %>%
			as.numeric(.) %>%
			tibble(degree = degree, jobs = .) %>%
			return(.)
		}) %>%
	dplyr::bind_rows(.)


openxlsx::read.xlsx('https://www2.census.gov/programs-surveys/popest/tables/2010-2019/counties/totals/co-est2019-annres.xlsx', startRow = 4) %>% as_tibble(.) %>% dplyr::transmute(., county = str_replace(X1, coll('.'), ''), population = .$'2019') %>% dplyr::filter(., county != 'United States') %>% 