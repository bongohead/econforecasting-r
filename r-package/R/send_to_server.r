#' A devops utility function to upload project from dev to production server
#'
#'
#'
#' @export
send_to_server = function() {

	# Clean out directory
	paste0(
		'"C:\\Program Files\\Git\\bin\\bash.exe"',
		' -c "',
		'ssh -t charles@econforecasting.com',
		' -i C:/Users/Charles/.ssh/id_rsa',
		' \'rm -rf /home/charles/projects/econforecasting/*\'',
		'"'
		) %>%
		system(.)

	# SCP files over
	paste0(
		'"C:\\Program Files\\Git\\bin\\bash.exe"',
		' -c "',
		'scp ',
		' -i C:/Users/Charles/.ssh/id_rsa',
		' -rp D:/OneDrive/__Projects/econforecasting/* charles@econforecasting.com:/home/charles/projects/econforecasting',
		'"'
		) %>%
		system(.)

	# Install package
	paste0(
		'"C:\\Program Files\\Git\\bin\\bash.exe"',
		' -c "',
		'ssh -t charles@econforecasting.com',
		' -i C:/Users/Charles/.ssh/id_rsa',
		' \'R CMD INSTALL /home/charles/projects/econforecasting/r-package/*\'',
		'"'
		) %>%
		system(.)
}
