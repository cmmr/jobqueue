

u_wait <- function (self, private, state, timeout, signal) {
  
  state   <- validate_string(state)
  timeout <- validate_positive_number(timeout)
  signal  <- validate_logical(signal)
  
  if (!is.null(timeout) && timeout < Inf) {
    msg <- '{private$.uid} took longer than {.val {timeout}} second{?s} to enter {.val {state}} state'
    msg <- cli_fmt(cli_text(msg))
    clear_timeout <- later(~{ self$stop(msg, 'timeout') }, delay = timeout)  # nocov
  } else {
    clear_timeout <- invisible
  }
  
  if (state %in% c('*', '.next')) {
    curr <- private$.state
    while (TRUE) {
      run_now(timeoutSecs = 0.2)
      if (private$.state != curr) break
      if (private$.is_done)       break
    }
  }
  else {
    while (TRUE) {
      if (private$.state == state) break
      if (private$.is_done)        break
      run_now(timeoutSecs = 0.2)
    }
  }
  
  clear_timeout()
  
  if (is_true(signal) && !is.null(private$.cnd)) {
    if (inherits(private$.cnd, 'error'))
      cnd_signal(private$.cnd)
    abort(private$.cnd$message, use_cli_format = FALSE)
  }
  
  return (invisible(self))
}


u_on <- function (self, private, prefix, state, func) {
  
  state <- validate_string(state)
  func  <- validate_function(func, null_ok = FALSE)
  
  uid <- attr(func, '.uid') <- increment_uid(prefix)
  private$.hooks %<>% c(set_names(list(func), state))
  
  off <- function () private$.hooks %<>% attr_ne('.uid', uid)
  
  if (state %in% c('*', self$state)) func(self)
  
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


run_job_function <- function (value, job) {
  if (is_formula(value)) value <- as_function(value)
  if (inherits(job, 'job'))
    if (is_function(value)) value <- value(job)
  return (value)
}


as_cnd <- function (reason, cls) {
  if (inherits(reason, 'condition')) return (reason)
  rlang::cnd(
    class   = unique(validate_character_vector(cls)), 
    message = validate_string(reason) )
}


as_error <- function (reason) {
  cnd <- as_cnd(reason, 'error')
  if (!inherits(cnd, 'error')) {
    cls <- class(cnd)
    cls <- append(cls, 'error', which(cls == 'condition') - 1)
    class(cnd) <- cls
  }
  return (cnd)
}



increment_uid <- function (prefix) {
  hash  <- paste0('prefix_', rlang::hash(prefix))
  value <- env_get(ENV, hash, 1L)
  assign(hash, value + 1L, ENV)
  return (paste0(prefix, value))
}


read_logs <- function (tmp) {
  
  logs <- NULL
  
  for (stream in c('stdout', 'stderr')) {
    fp <- file.path(tmp, paste0(stream, '.txt'))
    if (is_true(file.exists(fp)) && is_true(file.size(fp) > 0))
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
    key <- names(value)[[idx]] %||% ''
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



# Finds e.g. '/path/to/pid_afNUaEVguc0l' and returns 'afNUaEVguc0l'
dir_prefix <- function (dir, prefix) {
  
  file <- list.files(dir, pattern = paste0('^', prefix))
  if (length(file) == 1)
    return (substr(file, nchar(prefix) + 1, nchar(file)))
  
  return (NULL)
}

dir_ps <- function (dir) {
  if (!is.null(ps <- dir_prefix(dir, 'pid_')))
    ps <- try(silent = TRUE, p__ps_handle(ps))
  return (ps)
}

dir_sem <- function (dir) {
  if (!is.null(sem <- dir_prefix(dir, 'sem_')))
    sem <- interprocess::semaphore(name = sem, assert = 'exists')
  return (sem)
}

dir_create <- function (dir, inc = NULL) {
  if (!is.null(inc))    dir <- c(dir, increment_uid(inc))
  if (length(dir) > 1)  dir <- do.call(file.path, as.list(dir))
  if (!dir.exists(dir)) dir.create(dir, recursive = TRUE)
  normalizePath(dir, winslash = '/')
}

dir_delete <- function (dir) {
  if (dir.exists(dir))
    unlink(dir, recursive = TRUE, force = TRUE, expand = FALSE)
}

dir_cleanup <- function (dir) {
  files <- list.files(dir, all.files = TRUE, no.. = TRUE)
  files <- setdiff(files, c('cmds.r', 'stdout.txt', 'stderr.txt', 'ready'))
  files <- file.path(dir, files)
  if (length(files) > 0)
    unlink(files, recursive = TRUE, force = TRUE, expand = FALSE)
}

file_create <- function (dir, file) {
  if (length(file) > 1) file <- paste(file, collapse = '')
  file.create(file.path(dir, file))
}

file_exists <- function (dir, file) {
  file.exists(file.path(dir, file))
}

is_cran_check <- function() {
  if (identical(Sys.getenv("NOT_CRAN"), "true")) {
    FALSE
  } else {
    Sys.getenv("_R_CHECK_PACKAGE_NAME_", "") != ""
  }
}
