

u_wait <- function (self, private, state) {
  
  state <- validate_string(state)
  curr  <- private$.state
  
  if (state != curr) {
    prev <- curr
    while (TRUE) {
      curr <- private$.state
      if (curr != prev)
        if (state %in% c('*', '.next', curr))
          break
      run_now(timeoutSecs = 0.2)
    }
  }
  
  return (invisible(self))
}


u_on <- function (self, private, prefix, state, func) {
  
  state <- validate_string(state)
  func  <- validate_function(func, null_ok = FALSE)
  
  uid <- attr(func, '.uid') <- increment_uid(prefix)
  private$.hooks %<>% c(set_names(list(func), state))
  
  off <- function () private$.hooks %<>% attr_ne('.uid', uid)
  
  if (state == self$state) func(self)
  if (state == '*')        func(self)
  
  return (invisible(off))
}



u__set_state <- function (self, private, state) {
  
  if (private$.state != state) {
    
    private$.state <- state
    hooks          <- private$.hooks
    private$.hooks <- hooks[names(hooks) != '.next']
    hooks          <- hooks[names(hooks) %in% c('*', '.next', state)]
    
    # cnd <- catch_cnd({
      for (i in seq_along(hooks)) {
        func <- hooks[[i]]
        
        if (!is_null(formals(func))) { func(self) }
        else if (is.primitive(func)) { func(self) }
        else                         { func()     }
      }
    # })
    # 
    # if (!is_null(cnd)) self$stop(cnd)
  }
  
  return (invisible(NULL))
}


interrupt_cnd <- function (reason = 'stopped', cls = NULL) {
  reason <- validate_string(reason, cnd_ok = TRUE)
  cls    <- validate_character_vector(cls)
  cnd(c(cls, 'interrupt'), message = as.character(reason))
}



last_uid <- new_environment()

increment_uid <- function (prefix) {
  value <- env_get(last_uid, prefix, 1L)
  assign(prefix, value + 1L, last_uid)
  return (paste0(prefix, value))
}


# Two-step save.
save_rds <- function (tmp, ...) {
  
  dots <- list(...)
  for (i in seq_along(dots)) {
    
    # save_rds(tmp, output = output)
    key <- names(dots)[[i]] %||% ''
    val <- dots[[i]]
    
    # save_rds(tmp, 'output')
    if (nchar(key) == 0) {
      key <- dots[[i]]
      val <- get(key, pos = parent.frame())
    }
    tmp_dest <- file.path(tmp, paste0('_', key, '.rds'))
    dest     <- file.path(tmp, paste0(     key, '.rds'))
    
    saveRDS(val, tmp_dest)
    file.rename(tmp_dest, dest)
  }
}


read_logs <- function (tmp) {
  
  logs <- NULL
  
  for (stream in c('stdout', 'stderr')) {
    fp <- file.path(tmp, paste(stream, '.txt'))
    if (isTRUE(file.exists(fp)) && isTRUE(file.size(fp) > 0))
      logs %<>% c(sprintf('%s>  %s', stream, readLines(fp)))
  }
  
  return (logs)
}

coan <- function (x) {
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
    if (nzchar(key)) { varname <- paste0(varname, '$', coan(key))  }
    else             { varname <- paste0(varname, '[[', idx, ']]') }
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

