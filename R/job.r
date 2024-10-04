
#' Define a Job to Run on a Background Worker Process.
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
#' @param tmax  A named numeric vector indicating the maximum number of 
#'        seconds allowed for each state the job passes through, or 'total' to
#'        apply a single timeout from 'submitted' to 'done'. Example:
#'        `tmax = c(total = 2.5, running = 1)` will force-stop a job 2.5 
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
#'            \item{`'submitted'` -  }{ Before `stop_id` and `copy_id` are resolved. }
#'            \item{`'queued'` -     }{ After `<Job>$queue` is assigned. }
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
    initialize = function (expr, vars = NULL, tmax = NULL, hooks = NULL, reformat = TRUE, cpus = 1L)
      j_initialize(self, private, expr, vars, tmax, hooks, reformat, cpus),
    
    #' @description
    #' Print method for a Job.
    #' @param ... Arguments are not used currently.
    #' @return This Job, invisibly.
    print = function (...) j_print(self),
    
    #' @description
    #' Attach a callback function.
    #' @return A function that when called removes this callback from the Job.
    on = function (state, func) j_on(self, private, state, func),
    
    #' @description
    #' Stop this Job. If the Job is running, the worker process will be rebooted.
    #' @return This Job, invisibly.
    stop = function (reason = 'job stopped by user') j_stop(self, private, reason)
  ),
  
  private = list(
    
    .expr     = NULL,
    .vars     = NULL,
    .cpus     = NULL,
    .tmax     = list(),
    .hooks    = list(),
    .reformat = list(),
    
    .uid      = NULL,
    .state    = 'initializing',
    .proxy    = NULL,
    .is_done  = FALSE,
    .output   = NULL
  ),
  
  active = list(
    
    #' @field
    #' Get the expression that will be run by this Job.
    expr = function () private$.expr,
    
    #' @field
    #' Get or set the variables that will be placed into the expression's 
    #' environment before evaluation.
    vars = function (value) j_vars(private, value),
    
    #' @field
    #' Get or set the `function (job, output)` for transforming raw `callr` 
    #' output to the Job's result.
    reformat = function (value) j_reformat(private, value),
    
    #' @field
    #' Get or set the number of CPUs to reserve for evaluating `expr`.
    cpus = function (value) j_cpus(private, value),
    
    #' @field
    #' Get or set the time limits to apply to this Job.
    tmax = function (value) j_tmax(self, private, value),
    
    #' @field
    #' Get or set the Job to proxy in place of running `expr`.
    proxy = function (value) j_proxy(self, private, value),
    
    #' @field
    #' Get or set the Job's state (setting will trigger callbacks).
    state = function (value) j_state(self, private, value),
    
    #' @field
    #' Get or set the Job's raw `callr` output (setting will change the Job's 
    #' state to 'done').
    output = function (value) j_output(self, private, value),
    
    #' @field
    #' Get the result of `expr`. Will block until Job is finished.
    result = function () j_result(self, private),
    
    #' @field
    #' Get all currently registered callback hooks - a named list of functions.
    hooks = function () private$.hooks,
    
    #' @field
    #' Returns TRUE or FALSE depending on if the Job's result is ready.
    is_done = function () private$.is_done,
    
    #' @field
    #' Returns a short string, e.g. 'J16', that uniquely identifies this Job.
    uid = function () private$.uid
  )
)


# Sanitize and track values for later use.
j_initialize <- function (self, private, expr, vars, tmax, hooks, reformat, cpus) {
  
  expr_subst    <- substitute(expr, env = parent.frame())
  private$.expr <- validate_expression(expr, expr_subst, null_ok = FALSE)
  
  self$vars     <- vars
  self$tmax     <- tmax
  self$reformat <- reformat
  self$cpus     <- cpus
  
  private$.uid   <- increment_uid('J')
  private$.hooks <- validate_hooks(hooks, 'JH')
  
  self$state <- 'created'
  
  return (self)
}


j_print <- function (self) {
  cli_text('{self$uid} {.cls {class(self)}} [{self$state}]')
  return (invisible(off))
}


j_on <- function (self, private, state, func) {
  
  state <- validate_string(state)
  func  <- validate_function(func, null_ok = FALSE)
  
  uid <- attr(func, '.uid') <- increment_uid('JH')
  private$.hooks %<>% c(setNames(list(func), state))
  
  off <- function () private$.hooks %<>% attr_ne('.uid', uid)
  
  if (state == self$state) func(self)
  if (state == '*')        func(self)
  
  return (invisible(off))
}


j_stop <- function (self, private, reason) {
  # cli_text('Job $stop() called with reason: {.val {reason}}')
  self$output <- list('error' = interrupted(reason))
  return (invisible(self))
}


# blocking
j_output <- function (self, private, value) {
  
  if (missing(value)) {
    while (!private$.is_done) run_now(timeoutSecs = 0.2)
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
  
  if (!is_null(output[['error']]))    { result <- output[['error']]  }
  else if (hasName(output, 'result')) { result <- output[['result']] }
  else                                { result <- output             }
  
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
  
  state <- validate_string(value)
  
  # cli_text('Job {self$uid} state change: {private$.state} -> {state}')
  
  if (state == private$.state) return (NULL)
  if (private$.is_done == 'done')
    cli_abort("Job$state can't be changed from 'done' to '{state}'.")
  if (state == 'done' && !private$.is_done)
    cli_abort("Job$state can't be set to 'done' until Job$output is set.")
  
  # Start the 'total' timeout when we leave the 'created' state.
  if (private$.state == 'created')
    if (!is_null(tmax <- private$.tmax[['total']])) {
      msg <- 'total runtime exceeded {tmax} second{?s}'
      msg <- cli_fmt(cli_text(msg))
      self$on('done', later(~self$stop(msg), delay = tmax))
    }
  
  private$.state <- state
  
  hooks          <- private$.hooks
  private$.hooks <- hooks[names(hooks) != '.next']
  hooks          <- hooks[names(hooks) %in% c('*', '.next', state)]
  
  for (i in seq_along(hooks)) {
    func <- hooks[[i]]
    # uid  <- attr(func, '.uid', exact = TRUE)
    # cli_text('Executing Job {self$uid} {.val {state}} callback hook {uid}.')
    if (!is_null(formals(func))) { func(self) }
    else if (is.primitive(func)) { func(self) }
    else                         { func()     }
  }
  
  # Start the timeout for this state, if present.
  if (!is_null(tmax <- private$.tmax[[state]])) {
    msg <- 'exceeded {.val {tmax}} second{?s} while in {.val {state}} state'
    msg <- cli_fmt(cli_text(msg))
    clear_timeout <- later(~{ self$stop(msg) }, delay = tmax)
    self$on('.next', function (job) { clear_timeout() })
  }
}



# Active bindings to validate any changes to "public" values.
j_vars <- function (private, value) {
  if (missing(value)) return (private$.vars)
  private$.vars <- validate_list(value)
}

j_tmax <- function (self, private, value) {
  if (missing(value)) return (private$.tmax)
  private$.tmax <- validate_tmax(value)
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
