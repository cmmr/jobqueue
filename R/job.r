
#' Define a Job in Isolation from Queues/Workers.
#' 
#' 
#'
#' @name Job
#'
#' @param expr  A call or R expression wrapped in curly braces to evaluate on a 
#'        worker. Will have access to any variables defined by `vars`, as well 
#'        as the Worker's `globals`, `packages`, and `init` configuration.
#' 
#' @param vars  A list of named variables to make available to `expr` during 
#'        evaluation.
#' 
#' @param scan  Should additional variables be added to `vars` based on 
#'        scanning `expr` for missing global variables? The default, 
#'        `scan = TRUE` always scans and adds to `vars`, set `scan = FALSE` to 
#'        never scan, and `scan = <an environment-like object>` to look for 
#'        globals there. `vars` defined by the user are always left untouched.
#' 
#' @param ignore  A character vector of variable names that should NOT be added 
#'        to `vars` when `scan=TRUE`.
#' 
#' @param timeout  A named numeric vector indicating the maximum number of 
#'        seconds allowed for each state the job passes through, or 'total' to
#'        apply a single timeout from 'submitted' to 'done'. Example:
#'        `timeout = c(total = 2.5, running = 1)` will force-stop a job 2.5 
#'        seconds after it is submitted, and also limits its time in the 
#'        running state to just 1 second.
#'        
#' @param hooks  A list of functions to run when the Job state changes, of the 
#'        form `hooks = list(created = function (job) {...}, done = ~{...})`.
#'        The names of these functions should be `created`, `submitted`, 
#'        `queued`, `dispatched`, `starting`, `running`, `done`, or `'*'`. 
#'        `'*'` will be run every time the state changes, whereas the others 
#'        will only be run when the Job enters that state. Duplicate names are 
#'        allowed.
#'        
#' @param reformat  The underlying call to `callr::r_session$call()` returns
#'        information on stdout, stderr, etc. When `reformat=TRUE` (the 
#'        default), only the result of the expression is returned. Set 
#'        `reformat=FALSE` to return the entire callr output, or 
#'        `reformat=function(job,output)` to use a function of your own to 
#'        post-process the output from callr.
#'        
#' @param cpus  How many CPU cores to reserve for this Job. The [Queue] uses 
#'        this number to limit the number of simultaneously running Jobs; it 
#'        does not prevent a Job from using more CPUs than reserved.
#'        
#' @param state  The Job state that will trigger this function. Typically one of:
#'        \describe{
#'            \item{`'*'` -          }{ Every time the state changes. }
#'            \item{`'.next'` -      }{ Only one time, the next time the state changes. }
#'            \item{`'created'` -    }{ After `Job$new()` initialization. }
#'            \item{`'submitted'` -  }{ After `<Job>$queue` is assigned. }
#'            \item{`'queued'` -     }{ After `stop_id` and `copy_id` are resolved. }
#'            \item{`'dispatched'` - }{ After `<Job>$worker` is assigned. }
#'            \item{`'starting'` -   }{ Before evaluation begins. }
#'            \item{`'running'` -    }{ After evaluation begins. }
#'            \item{`'done'` -       }{ After `<Job>$output` is assigned. }
#'        }
#'        
#' @param func  A function that accepts a Job object as input. You can call 
#'        `<Job>$stop()` or edit its values and the changes will be persisted 
#'        (since Jobs are reference class objects). You can also edit/stop 
#'        other queued jobs by modifying the Jobs in `<Job>$queue$jobs`. 
#'        Return value is ignored.
#'        
#' @param reason  A message or other value to include in the 'interrupt' 
#'        condition object that will be returned as the Job's result.
#'        
#' @param ...  Arbitrary named values to add to the returned Job object.
#'
#' @export
#' 

Job <- R6Class(
  classname    = "Job",
  cloneable    = FALSE,
  lock_objects = FALSE,
  
  public = list(
    
    #' @description
    #' Creates a Job object defining how to run an expression on a background worker process.
    #' @return A Job object.
    initialize = function (
        expr, 
        vars     = NULL, 
        scan     = TRUE, 
        ignore   = NULL, 
        envir    = parent.frame(),
        timeout  = NULL, 
        hooks    = NULL, 
        reformat = TRUE, 
        cpus     = 1L,
        ... ) {
      
      j_initialize(
        self, private, 
        expr, vars, scan, ignore, envir, 
        timeout, hooks, reformat, cpus, ... )
    },
    
    #' @description
    #' Print method for a Job.
    #' @param ... Arguments are not used currently.
    #' @return This Job, invisibly.
    print = function (...) j_print(self),
    
    #' @description
    #' Attach a callback function.
    #' @return A function that when called removes this callback from the Job.
    on = function (state, func) u_on(self, private, 'JH', state, func),
    
    #' @description
    #' Blocks until the Job enters the given state.
    #' @return This Job, invisibly.
    wait = function (state = 'done') u_wait(self, private, state),
    
    #' @description
    #' Stop this Job. If the Job is running, the worker process will be rebooted.
    #' @return This Job, invisibly.
    stop = function (reason = 'job stopped by user') j_stop(self, private, reason)
  ),
  
  private = list(
    
    .expr     = NULL,
    .vars     = NULL,
    .scan     = NULL,
    .ignore   = NULL,
    .envir    = NULL,
    .cpus     = NULL,
    .timeout     = list(),
    .hooks    = list(),
    .reformat = list(),
    
    .uid      = NULL,
    .state    = 'initializing',
    .proxy    = NULL,
    .is_done  = FALSE,
    .output   = NULL
  ),
  
  active = list(
    
    #' @field expr
    #' Get the expression that will be run by this Job.
    expr = function () private$.expr,
    
    #' @field vars
    #' Get or set the variables that will be placed into the expression's 
    #' environment before evaluation.
    vars = function (value) j_vars(private, value),
    
    #' @field scan
    #' Get or set whether to scan for missing `vars`.
    scan = function (value) j_scan(private, value),
    
    #' @field ignore
    #' Get or set a character vector of variable names to NOT add to `vars`.
    ignore = function (value) j_ignore(private, value),
    
    #' @field envir
    #' Get or set the environment where missing `vars` can be found.
    envir = function (value) j_envir(private, value),
    
    #' @field reformat
    #' Get or set the `function (job, output)` for transforming raw `callr` 
    #' output to the Job's result.
    reformat = function (value) j_reformat(private, value),
    
    #' @field cpus
    #' Get or set the number of CPUs to reserve for evaluating `expr`.
    cpus = function (value) j_cpus(private, value),
    
    #' @field timeout
    #' Get or set the time limits to apply to this Job.
    timeout = function (value) j_timeout(self, private, value),
    
    #' @field proxy
    #' Get or set the Job to proxy in place of running `expr`.
    proxy = function (value) j_proxy(self, private, value),
    
    #' @field state
    #' Get or set the Job's state (setting will trigger callbacks).
    state = function (value) j_state(self, private, value),
    
    #' @field output
    #' Get or set the Job's raw `callr` output (assigning to `$output` will 
    #' change the Job's state to 'done').
    output = function (value) j_output(self, private, value),
    
    #' @field result
    #' Result of `expr`. Will block until Job is finished.
    result = function () j_result(self, private),
    
    #' @field hooks
    #' Currently registered callback hooks as a named list of functions.
    hooks = function () private$.hooks,
    
    #' @field is_done
    #' Returns TRUE or FALSE depending on if the Job's result is ready.
    is_done = function () private$.is_done,
    
    #' @field uid
    #' A short string, e.g. 'J16', that uniquely identifies this Job.
    uid = function () private$.uid
  )
)


# Sanitize and track values for later use.
j_initialize <- function (self, private, expr, vars, scan, ignore, envir, timeout, hooks, reformat, cpus, ...) {
  
  expr_subst    <- substitute(expr, env = parent.frame())
  private$.expr <- validate_expression(expr, expr_subst, null_ok = FALSE)
  
  dots <- dots_list(..., .named = TRUE)
  for (i in names(dots))
    self[[i]] <- dots[[i]]
  
  if (is_null(envir))
    envir <- parent.frame(n = 2)
  
  self$vars     <- vars
  self$scan     <- scan
  self$ignore   <- ignore
  self$envir    <- envir
  self$timeout  <- timeout
  self$reformat <- reformat
  self$cpus     <- cpus
  
  private$.uid   <- increment_uid('J')
  private$.hooks <- validate_hooks(hooks, 'JH')
  
  self$state <- 'created'
  
  return (self)
}


j_print <- function (self) {
  cli_text('{self$uid} {.cls {class(self)}} [{self$state}]')
  return (invisible(self))
}


j_stop <- function (self, private, reason) {
  # cli_text('Job $stop() called with reason: {.val {reason}}')
  self$output <- list('error' = interrupted(reason))
  return (invisible(self))
}


# blocking
j_output <- function (self, private, value) {
  
  if (missing(value)) {
    
    if (self$state == 'created')
      if (inherits(self$queue, 'Queue'))
        self$queue$submit(self)
    
    self$wait() # blocking
    return (private$.output)
  }
  
  # cli_text('Output recieved for job {self$uid}: {.type {value}}')
  
  # Only accept the first assignment to Job$output
  if (!private$.is_done) {
    # cli_text('Output assigned to job {self$uid}: {.type {value}}')
    private$.proxy   <- NULL
    private$.is_done <- TRUE
    private$.output  <- value
    self$state       <- 'done'
  }
  
}


# Reformat the raw 'callr_session_result' every call.
j_result <- function (self, private) {
  
  output   <- self$output # blocking
  reformat <- private$.reformat
  
  if (is_false(reformat))    return (output) 
  if (is_function(reformat)) return (reformat(output))
  
  if (hasName(output, 'error') && !is_null(output[['error']])) {
    result <- output[['error']]
  }
  else if (hasName(output, 'result')) { 
    result <- output[['result']]
  }
  else {
    result <- output
  }
  
  return (result)
}


# Mirror another Job's output.
j_proxy <- function (self, private, value) {
  
  if (missing(value)) return (private$.proxy)
  
  if (!inherits(value, 'Job'))
    cli_abort('Job$proxy must be a Job, not {.type {value}}.')
  
  if (!private$.is_done) {
    private$.proxy <- value
    private$.proxy$on('done', function (job) {
      if (identical(self$proxy$uid, job$uid))
        self$output <- job$output
    })
  }
}


# Typically 'created', 'submitted', 'queued', 'dispatched', 
# 'starting', 'running', or 'done'.
j_state <- function (self, private, value) {
  
  if (!is_null(private$.proxy)) {
    if (missing(value)) return (paste(private$.proxy$state, 'by proxy'))
    cli_abort("Can't change state for proxied job.")
  }
  
  if (missing(value)) return (private$.state)
  
  new_state  <- validate_string(value)
  curr_state <- private$.state
  
  if (new_state == curr_state) return (NULL)
  if (curr_state == 'done')
    cli_abort("Job$state can't be changed from 'done' to '{new_state}'.")
  if (new_state == 'done' && !private$.is_done)
    cli_abort("Job$state can't be set to 'done' until Job$output is set.")
  
  private$.state <- new_state
  
  # Start the 'total' timeout when we enter the 'submitted' state.
  if (new_state == 'submitted')
    if (!is_null(timeout <- private$.timeout[['total']])) {
      msg <- 'total runtime exceeded {timeout} second{?s}'
      msg <- cli_fmt(cli_text(msg))
      self$on('done', later(~self$stop(msg), delay = timeout))
    }
  
  hooks          <- private$.hooks
  private$.hooks <- hooks[names(hooks) != '.next']
  hooks          <- hooks[names(hooks) %in% c('*', '.next', new_state)]
  
  for (i in seq_along(hooks)) {
    func <- hooks[[i]]
    if (!is_null(formals(func))) { func(self) }
    else if (is.primitive(func)) { func(self) }
    else                         { func()     }
  }
  
  # Start the timeout for this new state, if present.
  if (!is_null(timeout <- private$.timeout[[new_state]])) {
    msg <- 'exceeded {.val {timeout}} second{?s} while in {.val {new_state}} state'
    msg <- cli_fmt(cli_text(msg))
    clear_timeout <- later(~{ self$stop(msg) }, delay = timeout)
    self$on('.next', function (job) { clear_timeout() })
  }
}



# Active bindings to validate any changes to "public" values.
j_vars <- function (private, value) {
  if (missing(value)) return (private$.vars)
  private$.vars <- validate_list(value)
}

j_scan <- function (private, value) {
  if (missing(value)) return (private$.scan)
  private$.scan <- validate_logical(value)
}

j_ignore <- function (private, value) {
  if (missing(value)) return (private$.ignore)
  private$.ignore <- validate_character_vector(value)
}

j_envir <- function (private, value) {
  if (missing(value)) return (private$.envir)
  private$.envir <- validate_environment(value)
}

j_timeout <- function (self, private, value) {
  if (missing(value)) return (private$.timeout)
  private$.timeout <- validate_timeout(value)
}

j_reformat <- function (private, value) {
  if (missing(value)) return (private$.reformat)
  private$.reformat <- validate_function(value, bool_ok = TRUE, if_null = TRUE)
}

j_cpus <- function (private, value) {
  if (missing(value)) return (private$.cpus)
  private$.cpus <- validate_positive_integer(value, if_null = 1L)
}





#' @export
as.promise.Job <- function (x) {
  
  job <- x
  p   <- attr(job, '.jobqueue_promise', exact = TRUE)
  
  if (is_null(p)) {
    if (job$is_done) { p <- promise_resolve(job$result)                      }
    else             { p <- promise(~{ job$on('done', ~resolve(.$result)) }) }
    attr(job, '.jobqueue_promise') <- p
  }
  
  return (p)
}
