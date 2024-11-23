
validate_function <- function (value, bool_ok = FALSE, if_null = NULL, null_ok = TRUE) {
  
  if (is_function(value))                           return (value)
  if (is_null(value)           && is_true(null_ok)) return (if_null)
  if (is_scalar_logical(value) && is_true(bool_ok)) return (value)
  
  varname  <- substitute(value)
  errmsg   <- cant_cast('a function')
  if (!is_formula(value)) cli_abort(errmsg)
  
  on_error <- function (e) { cli_abort(c(errmsg, 'x' = as.character(e) )) }
  tryCatch(as_function(value), error = on_error, warning = on_error)
}

validate_expression <- function (value, subst, null_ok = TRUE) {
  
  if (isa(subst, '{'))           return (subst)
  if (isa(value, '{'))           return (value)
  if (isa(value, 'call'))        return (value)
  if (is_null(value) && null_ok) return (NULL)
  
  varname <- substitute(value)
  cli_abort(must_be(c('a call', 'an expression in curly braces')))
}

validate_list <- function (value, null_ok = TRUE, if_null = list(), of_type = NULL, named = TRUE, default = NULL) {
  
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
        cli_abort(idx_must_be(cli_fmt(cli_text('{.or {.val {of_type}}}'))))
  }
  
  if (!is_list(value)) value <- as.list(value)
  
  return (value)
}

validate_hooks <- function (hooks, prefix = 'H') {
  
  hooks <- validate_list(hooks)
  if (length(hooks) == 0) return (hooks)
  
  names(hooks) <- sub('^[qwj]_', '', names(hooks))
  
  for (i in seq_along(hooks)) {
    func <- validate_function(hooks[[i]], null_ok = FALSE)
    if (is_null(attr(func, '.uid', exact = TRUE)))
      attr(func, '.uid') <- increment_uid(prefix)
    hooks[[i]] <- func
  }
  return (hooks)
}


validate_timeout <- function (timeout) {
  
  timeout <- validate_list(timeout, default = 'total')
  
  if (length(dups <- unique(names(timeout)[duplicated(names(timeout))])))
    cli_abort('`timeout` cannot have duplicate names: {.val {dups}}')
  
  expected <- 'a single positive number or NULL'
  for (i in seq_along(timeout)) {
    
    key <- paste0('timeout$', coan(names(timeout)[[i]]), '')
    on_error <- function (e) cli_abort(c(
      cant_cast(expected, value = timeout[[i]], varname = key),
      'x' = as.character(e) ))
    
    val <- tryCatch(as.numeric(timeout[[i]]), error = on_error, warning = on_error)
    
    if (length(val) != 1) cli_abort('{`key`} must be {expected}, not {.type {timeout[[i]]}}')
    if (val <= 0)         cli_abort('{`key`} must be {expected}, not {.val  {timeout[[i]]}}')
    
    timeout[[i]] <- val
  }
  
  return (timeout)
}


validate_environment <- function (value, null_ok = TRUE, if_null = NULL) {
  varname <- substitute(value)
  
  if (is_null(value) && is_true(null_ok)) return (if_null)
  
  errmsg   <- cant_cast('an environment')
  on_error <- function (e) { cli_abort(c(errmsg, 'x' = as.character(e) )) }
  
  tryCatch(
    expr    = as_environment(value, parent = baseenv()), 
    error   = on_error, 
    warning = on_error )
}

validate_positive_integer <- function (value, if_null = NULL, null_ok = TRUE) {
  if (is_null(value) && is_true(null_ok)) return (if_null)
  varname  <- substitute(value)
  errmsg   <- must_be('a single positive integer')
  on_error <- function (e) cli_abort(c(errmsg, 'x' = as.character(e) ))
  
  tryCatch(
    expr = {
      value <- as.integer(value)
      stopifnot(length(value) == 1)
      stopifnot(is_true(value > 0))
      value
    }, 
    error   = on_error, 
    warning = on_error )
}

validate_character_vector <- function (value, if_null = NULL, bool_ok = FALSE) {
  if (is_null(value)) return (if_null)
  if (!anyNA(value)) {
    if (is_character(value))                          return (value)
    if (is_scalar_logical(value) && is_true(bool_ok)) return (value)
  }
  varname <- substitute(value)
  cli_abort(must_be('a character vector'))
}

validate_string <- function (value, cnd_ok = FALSE) {
  varname <- substitute(value)
  if (is_condition(value) && is_true(cnd_ok)) value <- value$message
  if (!is_scalar_character(value) || is_na(value) || !nzchar(value))
    cli_abort(must_be(ifelse(cnd_ok, 'a string or condition', 'a string')))
  return (value)
}

