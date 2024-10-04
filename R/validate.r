
validate_function <- function (value, bool_ok = FALSE, if_null = NULL, null_ok = TRUE) {
  if (is_function(value))                           return (value)
  if (is_null(value)           && is_true(null_ok)) return (if_null)
  if (is_scalar_logical(value) && is_true(bool_ok)) return (value)
  varname <- substitute(value)
  errmsg  <- cant_cast('a function')
  tryCatch(as_function(value), error = function (e) {
    cli_abort(c(errmsg, 'x' = as.character(e) ))})
}

validate_expression <- function (value, subst, null_ok = TRUE) {
  
  if (isa(subst, '{'))           return (subst)
  if (isa(value, '{'))           return (value)
  if (isa(value, 'call'))        return (value)
  if (is_null(value) && null_ok) return (NULL)
  
  varname <- substitute(value)
  cli_abort(must_be(c('a call', 'an expression in curly braces')))
}


validate_list <- function (value, null_ok = TRUE, if_null = NULL, of_type = NULL, named = TRUE, default = NULL) {
  
  varname <- substitute(value)
  
  if (is_null(value) && is_true(null_ok)) return (if_null)
  
  if (!is_null(default))
    if (length(value) == 1 && !is_named(value))
      names(value) <- default
  
  if (is_true(named) && !is_named2(value))
    cli_abort(cannot('have missing element names'))
  
  if (is_false(named) && !is_null(names(value)))
    cli_abort(cannot('be named'))
  
  if (!is_null(of_type)) {
    if (identical(of_type, 'numeric')) of_type %<>% c('integer')
    for (i in seq_along(value))
      if (!inherits(value[[i]], of_type))
        idx_must_be(cli_fmt(cli_text('{.or {.val {of_type}}}')))
  }
  
  if (!is_list(value)) value <- as.list(value)
  
  return (value)
}

validate_hooks <- function (hooks, prefix = 'H') {
  hooks <- validate_list(hooks)
  for (i in seq_along(hooks)) {
    func <- validate_function(hooks[[i]], null_ok = FALSE)
    if (is_null(attr(func, '.uid', exact = TRUE)))
      attr(func, '.uid') <- increment_uid(prefix)
    hooks[[i]] <- func
  }
  return (hooks)
}


validate_tmax <- function (tmax) {
  
  if (length(tmax) == 1 && !is_named(tmax)) names(tmax) <- 'total'
  tmax <- validate_list(tmax)
  
  if (length(dups <- unique(names(tmax)[duplicated(names(tmax))])))
    cli_abort('`tmax` cannot have duplicate names: {.val {dups}}')
  
  expected <- 'a single positive number or NULL'
  for (i in seq_along(tmax)) {
    
    key <- paste0('`tmax$', coan(names(tmax)[[i]]), '`')
    val <- tryCatch(
      expr  = as.numeric(tmax[[i]]), 
      error = function (e) cli_abort(c(
        cant_cast(expected, value = tmax[[i]], varname = key),
        'x' = as.character(e) )))
    
    if (length(val) != 1) cli_abort('{key} must be {expected}, not {.type {tmax[[i]]}}')
    if (val <= 0)         cli_abort('{key} must be {expected}, not {.val  {tmax[[i]]}}')
    
    tmax[[i]] <- val
  }
  
  return (tmax)
}


validate_environment <- function (value) {
  varname <- substitute(value)
  errmsg  <- cant_cast('an environment')
  tryCatch(as_environment(value), error = function (e) {
    cli_abort(c(errmsg, 'x' = as.character(e) ))})
}

validate_positive_numeric <- function (value, if_null = NULL, null_ok = TRUE) {
  if (is_null(value) && is_true(null_ok)) return (if_null)
  varname <- substitute(value)
  errmsg  <- must_be('a single positive number')
  tryCatch(
    expr = {
      value <- as.numeric(value)
      stopifnot(length(value) == 1)
      stopifnot(is_true(value > 0))
      value
    }, 
    error = function (e) { cli_abort(errmsg) })
}

validate_positive_integer <- function (value, if_null = NULL, null_ok = TRUE) {
  if (is_null(value) && is_true(null_ok)) return (if_null)
  varname <- substitute(value)
  errmsg  <- must_be('a single positive integer')
  tryCatch(
    expr = {
      value <- as.integer(value)
      stopifnot(length(value) == 1)
      stopifnot(is_true(value > 0))
      value
    }, 
    error = function (e) { cli_abort(errmsg) })
}

validate_logical <- function (value) {
  if (is_scalar_logical(value) && !is_na(value)) return (value)
  varname <- substitute(value)
  cli_abort(must_be('a character vector'))
}

validate_character_vector <- function (value, if_null = NULL) {
  if (is_null(value))                       return (if_null)
  if (is_character(value) && !anyNA(value)) return (value)
  varname <- substitute(value)
  cli_abort(must_be('a character vector'))
}

validate_file <- function (value, if_null = NULL, mustWork = TRUE) {
  if (is_null(value)) return (if_null)
  value <- normalizePath(value, winslash = '/', mustWork = mustWork)
  return (value)
}


validate_string <- function (value, null_ok = FALSE, zlen_ok = FALSE, na_ok = FALSE) {
  varname <- substitute(value)
  if (is_null(value) && is_true(null_ok))  return (value)
  if (is_na(value)   && is_true(na_ok))    return (value)
  if (!is_scalar_character(value))         cli_abort(must_be('a string'))
  if (!nzchar(value) && is_false(zlen_ok)) cli_abort(cannot('be ""'))
  return (value)
}

validate_string_options <- function (value, options) {
  
  if (is_string(value, options)) return (value)
  
  varname <- substitute(value)
  if (is_scalar_character(value))
    cli_abort(c('!' = "`{varname}` must be one of {.val {options}}, not {.val {value}}."))
  cli_abort(c('!' = "`{varname}` must be one of {.val {options}}, not {.type {value}}."))
}




