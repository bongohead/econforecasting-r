#' Imports secrets and validates keys
#'
#' @description
#' Imports secrets and validates keys are there.
#'
#' @param path_to_secrets_file The path to the secrets file.
#' @param validate_keys A character vector of secret keys to include.
#'
#' @importFrom yaml read_yaml
#' @export
get_secrets = function(path_to_secrets_file, validate_keys = c()) {
	if (!is.character(path_to_secrets_file)) stop('Parameter path_to_secrets_file must be a string')
	secrets = tryCatch(
		{yaml::read_yaml(path_to_secrets_file)},
		error = function(e) stop('Cannot read secrets file:\n ', e)
	)
	if (length(validate_keys) > 0) {
		missing_any = validate_keys[!validate_keys %in% names(secrets)]
		if (length(missing_any) > 0) stop('Missing in constants file : ', paste0(missing_any, collapse = ', '))
	}

	return(secrets)
}

#' Get secret secrets
#'
#' @description
#' Imports secrets and validates keys are there.
#'
#' @param key The key of the key to return
#' @param path_to_secrets_file The path to the secrets file.
#'
#' @importFrom yaml read_yaml
#'
#' @export
get_secret = function(key, path_to_secrets_file) {

	if (!is.character(key)) stop('Parameter path_to_secrets_file must be a string')
	if (!is.character(path_to_secrets_file)) stop('Parameter path_to_secrets_file must be a string')

	secrets = tryCatch(
		{yaml::read_yaml(path_to_secrets_file)},
		error = function(e) stop('Cannot read secrets file:\n ', e)
	)
	if (!key %in% names(secrets)) stop('Key does not exist in secrets file!')

	return(secrets[key])
}
