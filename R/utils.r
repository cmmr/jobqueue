
.jobqueue_env <- new_environment()


new_promise <- function () {
  promise(function (resolve, reject) { NULL })
}

noop <- function (...) return (invisible(NULL))

p_resolve  <- function (promise, value)  attr(promise, 'promise_impl')$resolve(value)
p_reject   <- function (promise, reason) attr(promise, 'promise_impl')$reject(reason)
p_resolver <- function (promise)         attr(promise, 'promise_impl')$resolve
p_rejecter <- function (promise)         attr(promise, 'promise_impl')$reject
p_state    <- function (promise)         attr(promise, 'promise_impl')$.__enclos_env__$private$state
p_result   <- function (promise)         attr(promise, 'promise_impl')$.__enclos_env__$private$value
p_visible  <- function (promise)         attr(promise, 'promise_impl')$.__enclos_env__$private$visible
p_pending  <- function (promise)         (p_state(promise) == 'pending')
p_done     <- function (promise)         (p_state(promise) != 'pending')
p_stop     <- function (promise, reason) p_resolve(promise, interrupted(reason))


interrupted <- function (reason = 'stopped') errorCondition(message = reason, class = 'interrupt')



increment_uid <- function (prefix, object = NULL) {
  if (!is_null(attr(object, '.uid', exact = TRUE)))
    return (attr(object, '.uid', exact = TRUE))
  nm    <- paste0('uid_', prefix)
  value <- env_get(.jobqueue_env, nm, 1L)
  assign(nm, value + 1L, .jobqueue_env)
  return (paste0(prefix, value))
}

coan <- function (x, i = NULL) {
  if (!is_null(i)) x <- names(x)[[i]]
  capture.output(as.name(x))
}

cannot <- function (excluded, key = NULL) {
  varname <- get('varname', pos = parent.frame())
  key <- ifelse(nzchar(key %||% ''), paste0('$', coan(key)), '')
  msg <- paste0('`{varname}{key}` cannot {excluded}.')
  return (c('!' = cli_fmt(cli_text(msg))))
}

must_be <- function (expected, value, varname) {
  if (missing(value))   value   <- env_get(parent.frame(), 'value')
  if (missing(varname)) varname <- env_get(parent.frame(), 'varname')
  null_ok <- env_get(parent.frame(), 'null_ok', FALSE)
  if (null_ok) expected %<>% c('NULL')
  not <- ifelse(is_null(value), 'NULL', paste0('{.type {value}}'))
  msg <- paste0('`{varname}` must be {.or {expected}}, not ', not, '.')
  return (c('!' = cli_fmt(cli_text(msg))))
}

cant_cast <- function (expected, value, varname) {
  if (missing(value))   value   <- env_get(parent.frame(), 'value')
  if (missing(varname)) varname <- env_get(parent.frame(), 'varname')
  
  msg <- "Can't convert `{varname}`, {.type {value}}, to {expected}."
  msg <- c('!' = cli_fmt(cli_text(msg)))
  
  alt_vals <- unique(c(
    if (is_true(env_get(parent.frame(), 'null_ok', FALSE))) c("NULL"),
    if (is_true(env_get(parent.frame(), 'true_ok', FALSE))) c("TRUE"),
    if (is_true(env_get(parent.frame(), 'bool_ok', FALSE))) c("TRUE", "FALSE") ))
  if (length(alt_vals) > 0) {
    alt_msg <- "`{varname}` can also be set to {.or {alt_vals}}."
    msg %<>% c('!' = cli_fmt(cli_text(alt_msg)))
  }
  
  return (msg)
}

idx_must_be <- function (expected) {
  varname <- env_get(parent.frame(), 'varname')
  value   <- env_get(parent.frame(), 'value')
  
  for (ij in c('i', 'j')) {
    if (!env_has(parent.frame(), ij)) break
    idx <- env_get(parent.frame(), ij, NULL)
    key <- names(value[[idx]]) %||% ''
    if (nzchar(key)) { varname <- paste(varname, '$', coan(key)) }
    else             { varname <- paste(varname, '[[', idx, ']]')  }
    value <- value[[idx]]
  }
  
  return (must_be(expected))
}


# Non-zero length
nz <- function (x) return (length(x) > 0)

# list x ==> c(x[[1]]$el, x[[2]]$el, ...)
map <- function (x, el) {
  unlist(sapply(seq_along(x), function (i) x[[i]][[el]]))
}

# list x ==> c(x[[1]]$f(...), x[[2]]$f(...), ...)
fmap <- function (x, f, ...) {
  unlist(sapply(seq_along(x), function (i) x[[i]][[f]](...)))
}

# Filter a list by element attribute value
attr_ne <- function (x, attr, val) {
  x[unlist(sapply(seq_along(x), function (i) !identical(base::attr(x[[i]], attr, exact = TRUE), val)))]
}

# Return elements from a list where identical(x[[i]]$el, val)
get_eq <- function (x, el, val) {
  x[unlist(sapply(seq_along(x), function (i) identical(x[[i]][[el]], val)))]
}

