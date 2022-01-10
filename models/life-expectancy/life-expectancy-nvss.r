
dt_raw =
	data.table::fread(
		file = 'C:/Users/Charles/Downloads/mort.txt', 
		sep = '|',
		col.names = 'str',
		strip.white = F
		)

str_strip_whitespace = function(x) {
	x1 = str_replace_all(x, coll(' '), '')
	x2 = ifelse(x1 == '', NA, x1) 
	return(x2)
}

dt_clean =
	dt_raw %>%
	.[1:100000, ] %>%
	.[, education := as.numeric(str_sub(str, 63, 63))] %>%
	.[, education := fcase(
		education %in% c(1, 2), 'no_hs',
		education == 3, 'hs',
		education == 4, 'some_college',
		education == 5, 'associates',
		education == 6, 'bachelors',
		education %in% c(7, 8), 'graduate',
		default = NA
	)] %>%
	.[, sex := str_to_lower(str_sub(str, 69, 69))] %>%
	.[, age := as.numeric(str_sub(str, 71, 73))] %>%
	.[, marital_status := str_to_lower(str_sub(str, 84, 84))] %>%
	.[, race := as.numeric(str_sub(str, 445, 446))] %>%
	.[, hispanic := as.numeric(str_sub(str, 484, 486))] %>%
	.[, hispanic := fcase(
		hispanic <= 199, 'not_hispanic',
		hispanic >= 200 & hispanic <= 995, 'hispanic',
		default = NA
	)] %>%
	.[, race := fcase(
		hispanic == 'hispanic', 'hispanic',
		race == 1, 'white',
		race == 2, 'black',
		race == 3, 'native_american',
		race == 5, 'chinese',
		race >= 4, 'asian'
		)] %>%
	.[, hispanic := NULL] %>%
	.[, date := ymd(paste0(str_sub(str, 102, 105), '-', str_sub(str, 65, 66), '-01'))] %>%
	.[, manner_of_death := str_sub(str, 107, 107)] %>%
	.[, manner_of_death := fcase(
		manner_of_death == 1, 'accident',
		manner_of_death %in% c(2, 6), 'suicide',
		manner_of_death == 3, 'homicide', 
		manner_of_death == 7, 'natural',
		default = NA
	)] %>%
	.[, place_of_death := as.numeric(str_sub(str, 83, 83))] %>%
	.[, place_of_death := fcase(
		place_of_death %in% 1:3, 'hospital',
		place_of_death %in% 4, 'home',
		place_of_death %in% 5:6, 'nursing_home',
		place_of_death %in% c(7, 9), 'unknown'
	)] %>%
	.[, activity_death := str_strip_whitespace(str_sub(str, 144, 144))] %>%
	
	as_tibble(.) %>%
	select(., -str) %>%
	print(.)
		