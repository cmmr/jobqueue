
#' How to Evaluate an R Expression
#' 
#' @name Job
#'
#' @description
#' 
#' The Job object defines several things:
#' 
#'   * The expression to run on a Worker process (`expr`).
#'   * What variables to make available during evaluation (`vars`).
#'   * How much time this task is allowed to take (`timeout`).
#'   * User-defined functions to run and when (`hooks`).
#'   * Result formatting (`reformat`).
#'   * Number of CPU cores to reserve (`cpus`).
#' 
#' *Typically you won't need to call `Job$new()`. Instead, create a [Queue] and use 
#' `<Queue>$run()` to generate Job objects.*
#'
#'
#' @param expr  A call or R expression wrapped in curly braces to evaluate on a 
#'        worker. Will have access to any variables defined by `vars`, as well 
#'        as the Worker's `globals`, `packages`, and `init` configuration.
#'        See `vignette('eval')`.
#' 
#' @param vars  A list of named variables to make available to `expr` during 
#'        evaluation.
#' 
#' @param timeout  A named numeric vector indicating the maximum number of 
#'        seconds allowed for each state the job passes through, or 'total' to
#'        apply a single timeout from 'submitted' to 'done'. Example:
#'        `timeout = c(total = 2.5, running = 1)`. See `vignette('stops')`.
#'        
#' @param hooks  A list of functions to run when the Job state changes, of the 
#'        form `hooks = list(created = function (job) {...}, done = ~{...})`.
#'        See `vignette('hooks')`.
#'        
#' @param reformat  Set `reformat = function (job)` to define what 
#'        `<Job>$result` should return. The default, `reformat = NULL` passes 
#'        `<Job>$output` unchanged to `<Job>$result`.
#'        See `vignette('results')`.
#'        
#' @param signal  Should calling `<Job>$result` signal on condition objects?
#'        When `FALSE`, `<Job>$result` will return the object without 
#'        taking additional action. Setting to `TRUE` or a character vector of 
#'        condition classes, e.g. `c('interrupt', 'error', 'warning')`, will
#'        cause the equivalent of `stop(<condition>)` to be called when those
#'        conditions are produced. See `vignette('results')`.
#'        
#' @param cpus  How many CPU cores to reserve for this Job. The [Queue] uses 
#'        this number to limit the number of simultaneously running Jobs; it 
#'        does not prevent a Job from using more CPUs than reserved.
#'        
#' @param state
#' The Job state that will trigger this function. Typically one of:
#'        
#' * `'*'` -          Every time the state changes.
#' * `'.next'` -      Only one time, the next time the state changes.
#' * `'created'` -    After `Job$new()` initialization.
#' * `'submitted'` -  After `<Job>$queue` is assigned.
#' * `'queued'` -     After `stop_id` and `copy_id` are resolved.
#' * `'dispatched'` - After `<Job>$worker` is assigned.
#' * `'starting'` -   Before evaluation begins.
#' * `'running'` -    After evaluation begins.
#' * `'done'` -       After `<Job>$output` is assigned.
#' 
#' Custom states can also be set/used.
#' 
#' @param func  A function that accepts a Job object as input. You can call 
#'        `<Job>$stop()` or edit its values and the changes will be persisted 
#'        (since Jobs are reference class objects). You can also edit/stop 
#'        other queued jobs by modifying the Jobs in `<Job>$queue$jobs`. 
#'        Return value is ignored.
#'        
#' @param reason  A message to include in the 'interrupt' condition object that 
#'        will be returned as the Job's result.
#'        
#' @param cls  Character vector of additional classes to prepend to 
#'        `c('interupt', 'condition')`.
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
        timeout  = NULL, 
        hooks    = NULL, 
        reformat = NULL, 
        signal   = FALSE, 
        cpus     = 1L,
        ... ) {
      
      j_initialize(
        self, private, 
        expr, vars, 
        timeout, hooks, reformat, signal, cpus, ... )
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
    stop = function (reason = 'job stopped by user', cls = NULL) j_stop(self, private, reason, cls)
  ),
  
  private = list(
    
    .expr     = NULL,
    .vars     = NULL,
    .cpus     = NULL,
    .timeout  = list(),
    .hooks    = list(),
    .reformat = NULL,
    .signal   = NULL,
    
    .uid      = NULL,
    .state    = 'initializing',
    .is_done  = FALSE,
    .output   = NULL,
    .proxy    = NULL,
    
    finalize = function () {
      if (identical(self$uid, self$worker$job$uid))
        if (!is_null(ps <- self$worker$ps))
          ps_kill(ps)
    }
  ),
  
  active = list(
    
    #' @field expr
    #' Get the expression that will be run by this Job.
    expr = function () private$.expr,
    
    #' @field vars
    #' Get or set the variables that will be placed into the expression's 
    #' environment before evaluation.
    vars = function (value) j_vars(private, value),
    
    #' @field reformat
    #' Get or set the `function (job)` for defining `<Job>$result`.
    reformat = function (value) j_reformat(private, value),
    
    #' @field signal
    #' Get or set the conditions to signal. 
    #' output to the Job's result.
    signal = function (value) j_signal(private, value),
    
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
    #' Get or set the Job's raw output (assigning to `$output` will 
    #' change the Job's state to `'done'`).
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
j_initialize <- function (
    self, private, expr, vars, timeout, hooks, reformat, signal, cpus, ...) {
  
  expr_subst    <- substitute(expr, env = parent.frame())
  private$.expr <- validate_expression(expr, expr_subst, null_ok = FALSE)
  
  dots <- dots_list(..., .named = TRUE)
  for (i in names(dots))
    self[[i]] <- dots[[i]]
  
  self$vars     <- vars
  self$timeout  <- timeout
  self$reformat <- reformat
  self$signal   <- signal
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


j_stop <- function (self, private, reason, cls) {
  self$output <- interrupt_cnd(reason, cls)
  return (invisible(self))
}


# blocking
j_output <- function (self, private, value) {
  
  if (missing(value)) {
    self$wait() # blocking
    return (private$.output)
  }
  
  # Only accept the first assignment to Job$output
  if (!private$.is_done) {
    private$.proxy   <- NULL
    private$.is_done <- TRUE
    private$.output  <- value
    self$state       <- 'done'
  }
  
}


# Reformat and/or signal `<Job>$output`.
j_result <- function (self, private) {
  
  reformat <- private$.reformat # NULL/function (job)
  
  if (is_null(reformat)) { result <- self$output    }
  else                   { result <- reformat(self) }
  
  signal <- private$.signal # TRUE/FALSE/c('interrupt', 'error')
  if (!is_false(signal) && inherits(result, 'condition'))
    if (is_true(signal) || any(signal %in% class(result)))
      cli_abort(
        .envir  = self$caller_env, 
        parent  = result,
        message = deparse1(private$.expr) )
  
  return (result)
}


# Mirror another Job's output.
j_proxy <- function (self, private, value) {
  
  if (missing(value)) return (private$.proxy)
  
  proxy <- value
  if (!is_null(proxy) && !inherits(proxy, 'Job'))
    cli_abort('proxy must be a Job or NULL, not {.type {proxy}}.')
  
  if (private$.is_done) return (NULL)
  
  private$.proxy <- proxy
  proxy$on('done', function (job) {
    if (identical(self$proxy$uid, job$uid))
      self$output <- job$output
  })
  
}


# Typically 'created', 'submitted', 'queued', 
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
  
  u__set_state(self, private, state = new_state)
  if (private$.state == 'done') return (NULL)
  
  # Start the 'total' timeout when we enter the 'submitted' state.
  if (new_state == 'submitted')
    if (!is_null(timeout <- private$.timeout[['total']])) {
      msg <- 'total runtime exceeded {timeout} second{?s}'
      msg <- cli_fmt(cli_text(msg))
      self$on('done', later(~self$stop(msg, 'timeout'), delay = timeout))
    }
  
  # Start the timeout for this new state, if present.
  if (!is_null(timeout <- private$.timeout[[new_state]])) {
    msg <- 'exceeded {.val {timeout}} second{?s} while in {.val {new_state}} state'
    msg <- cli_fmt(cli_text(msg))
    clear_timeout <- later(~{ self$stop(msg, 'timeout') }, delay = timeout)
    self$on('.next', function (job) { clear_timeout() })
  }
}



# Active bindings to validate any changes to "public" values.
j_vars <- function (private, value) {
  if (missing(value)) return (private$.vars)
  private$.vars <- validate_list(value)
}

j_timeout <- function (self, private, value) {
  if (missing(value)) return (private$.timeout)
  private$.timeout <- validate_timeout(value)
}

j_reformat <- function (private, value) {
  if (missing(value)) return (private$.reformat)
  private$.reformat <- validate_function(value)
}

j_signal <- function (private, value) {
  if (missing(value)) return (private$.signal)
  private$.signal <- validate_character_vector(value, bool_ok = TRUE)
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
    
    p <- promise(function (resolve, reject) {
      job$on('done', function (job) {
        cnd <- catch_cnd(result <- job$result)
        if (is_null(cnd)) { resolve(result) }
        else              { reject(cnd)     }
      })
    })
    
    attr(job, '.jobqueue_promise') <- p
  }
  
  return (p)
}
