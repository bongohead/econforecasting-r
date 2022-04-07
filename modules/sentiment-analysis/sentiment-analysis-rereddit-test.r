


test_jan_1 = lapply(1:200, function(page) {
	
	message(page)
	
	str_glue('https://www.reddit.com/posts/2021/january-1-{page}') %>%
		GET(.) %>%
		content(.) %>%
		html_nodes(., '#main div.DirectoryPost__Details') %>%
		map_dfr(., function(x) 
			tibble(
				subreddit = x %>% html_node(., '.DirectoryPost__Subreddit') %>% html_text(.),
				title = x %>% html_node(., '.DirectoryPost__Title') %>% html_text(.),
				)
			)
	})