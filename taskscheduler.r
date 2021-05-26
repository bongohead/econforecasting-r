DIR = 'D:/OneDrive/__Projects/econforecasting'


# Directory
sinkFile = file(file.path(DIR, 'sink.txt'), open = 'wt')
sink(sinkFile, type = 'output')
sink(sinkFile, type = 'message')


# Convert to R -> Run
lapply(list('model-asset-contagion', 'model-baseline-forecasts', 'model-nowcasts'), function(filename) {
	message('Started - ', filename)
	if (file.exists(paste0(DIR, '/', filename, '.r'))) file.remove(paste0(DIR, '/', filename, '.r'))
	knitr::purl(input = paste0(DIR, '/', filename, '.rmd'), output = paste0(DIR, '/', filename, '.r'))
	source(paste0(DIR, '/', filename, '.r'))
	file.remove(paste0(DIR, '/', filename, '.r'))
	message('Finished - ', filename)
})

sink()


# send.mail(
# 	from = sender,
# 	to = recipients,
# 	subject = "Subject of the email",
# 	body = "Body of the email",
# 	smtp = list(
# 		host.name = "smtp.gmail.com", port = 465, 
# 		user.name = "YOURUSERNAME@gmail.com",            
# 		passwd = "YOURPASSWORD",
# 		ssl = TRUE
# 		),
# 	authenticate = TRUE,
# 	send = TRUE
# 	)