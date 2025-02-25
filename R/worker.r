
#' A Background Process
#'
#' @name Worker
#'
#' @description
#' 
#' Where [Job] expressions are evaluated.
#' 
#' 
#' @param globals  A named list of variables that all `<Job>$expr`s will have 
#'        access to. Alternatively, an object that can be coerced to a named 
#'        list with `as.list()`, e.g. named vector, data.frame, or environment.
#' 
#' @param packages  Character vector of package names to load on workers.
#' 
#' @param namespace  The name of a package to attach to the worker's 
#'        environment.
#' 
#' @param init  A call or R expression wrapped in curly braces to evaluate on 
#'        each worker just once, immediately after start-up. Will have access 
#'        to variables defined by `globals` and assets from `packages` and 
#'        `namespace`. Returned value is ignored.
#'        
#' @param hooks  A named list of functions to run when the Worker state 
#'        changes, of the form `hooks = list(idle = function (worker) {...})`.
#'        Names of worker hooks are typically `starting`, `idle`, `busy`, 
#'        `stopped`, or `'*'` (duplicates okay). See `vignette('hooks')`.
#'        
#' @param wait  If `TRUE`, blocks until the Worker is 'idle'. If `FALSE`, the 
#'        Worker object is returned in the 'starting' state.
#'        
#' @param timeout  How long to wait for the worker to finish starting (in seconds).
#'        If `NA`, defaults to the `Worker$new()` argument.
#'        
#' @param job  A [Job] object, as created by `Job$new()`.
#'        
#' @param state
#' The name of a Worker state. Typically one of:
#' 
#' * `'*'` -        Every time the state changes.
#' * `'.next'` -    Only one time, the next time the state changes.
#' * `'starting'` - Waiting for the background process to load.
#' * `'idle'` -     Waiting for Jobs to be `$run()`.
#' * `'busy'` -     While a Job is running.
#' * `'stopped'` -  After `<Worker>$stop()` is called.
#' 
#'        
#' @param func  A function that accepts a Worker object as input. You can call 
#'        `<Worker>$stop()` and other `<Worker>$` methods.
#'        
#' @param reason  Passed to `<Job>$stop()` for any Jobs currently managed by 
#'        this Worker.
#'        
#' @param cls  Passed to `<Job>$stop()` for any Jobs currently managed by this 
#'        Worker.
#'
#' @export
#' 


Worker <- R6Class(
  classname    = "Worker",
  cloneable    = FALSE,
  lock_objects = FALSE,
  
  public = list(
    
    #' @description
    #' Creates a background R process for running [Job]s.
    #' @return A Worker object.
    initialize = function (
        globals   = NULL,
        packages  = NULL,
        namespace = NULL,
        init      = NULL,
        hooks     = NULL,
        wait      = TRUE,
        timeout   = Inf ) {
      
      w_initialize(self, private, globals, packages, namespace, init, hooks, wait, timeout)
    },
    
    
    #' @description
    #' Print method for a `Worker`.
    #' @param ... Arguments are not used currently.
    #' @return The Worker, invisibly.
    print = function (...) 
      w_print(self),
    
    #' @description
    #' Restarts a stopped Worker.
    #' @return The Worker, invisibly.
    start = function (wait = TRUE, timeout = NA) 
      w_start(self, private, wait, timeout),
    
    #' @description
    #' Stops a Worker by terminating the background process and calling 
    #' `<Job>$stop(reason)` on any Jobs currently assigned to this Worker.
    #' @return The Worker, invisibly.
    stop = function (reason = 'worker stopped by user', cls = NULL)
      w_stop(self, private, reason, cls),
    
    #' @description
    #' Restarts a Worker by calling `<Worker>$stop(reason)` and 
    #' `<Worker>$start()` in succession.
    #' @return The Worker, invisibly.
    restart = function (wait = TRUE, timeout = NA, reason = 'restarting worker', cls = NULL) 
      w_restart(self, private, wait, timeout, reason, cls),
    
    #' @description
    #' Attach a callback function to execute when the Worker enters `state`.
    #' @return A function that when called removes this callback from the Worker.
    on = function (state, func) 
      u_on(self, private, 'WH', state, func),
    
    #' @description
    #' Blocks until the Worker enters the given state.
    #' @param timeout Stop the Worker if it takes longer than this number of seconds.
    #' @param signal Raise an error if encountered (will also be recorded in `<Worker>$cnd`).
    #' @return This Worker, invisibly.
    wait = function (state = 'idle', timeout = Inf, signal = TRUE) 
      u_wait(self, private, state, timeout, signal),
    
    #' @description
    #' Assigns a Job to this Worker for evaluation on the background 
    #' process.
    #' @return This Worker, invisibly.
    run = function (job) 
      w_run(self, private, job)
  ),
  
  private = list(
    
    .hooks     = NULL,
    .state     = 'stopped',
    .is_done   = FALSE,
    .loaded    = NULL,
    .uid       = NULL,
    .job       = NULL,
    .ps        = NULL, # ps::ps_handle object
    .tmp       = NULL, # dir for child communication
    .cnd       = NULL,
    ps_tmp     = NULL, # child's tempdir()
    old_dirs   = NULL,
    semaphore  = NULL,
    frame      = NULL,
    trace      = NULL,
    config     = NULL,
    
    set_state    = function (state) u__set_state(self, private, state),
    poll_job     = function ()      w__poll_job(self, private),
    poll_startup = function ()      w__poll_startup(self, private),
    job_done     = function (job)   w__job_done(self, private, job),
    fp           = function (...)   file.path(private$.tmp, paste0(...)),
    
    finalize = function () {
      if (!is_null(ps <- private$.ps)) ps_kill(ps)
      old_dirs         <- c(private$old_dirs, private$.tmp, private$ps_tmp)
      private$old_dirs <- old_dirs[dir.exists(old_dirs)]
      lapply(private$old_dirs, unlink, recursive = TRUE, expand = FALSE)
    }
    
  ),
  
  active = list(
    
    #' @field hooks
    #' A named list of currently registered callback hooks.
    hooks = function () private$.hooks,
    
    #' @field job
    #' The currently running Job.
    job = function () private$.job,
    
    #' @field ps
    #' The `ps::ps_handle()` object for the background process.
    ps = function () private$.ps,
    
    #' @field state
    #' The Worker's state: `'starting'`, `'idle'`, `'busy'`, or `'stopped'`.
    state = function () private$.state,
    
    #' @field uid
    #' A short string, e.g. `'W11'`, that uniquely identifies this Worker.
    uid = function () private$.uid,
    
    #' @field tmp
    #' The Worker's temporary directory.
    tmp = function () private$.tmp,
    
    #' @field cnd
    #' The error that caused the Worker to stop.
    cnd = function () private$.cnd
  )
)


w_initialize <- function (self, private, globals, packages, namespace, init, hooks, wait, timeout) {
  
  init_subst <- substitute(init, env = parent.frame())
  
  wait <- validate_logical(wait)
  
  private$.uid    <- increment_uid('W')
  private$.hooks  <- validate_hooks(hooks, 'WH')
  private$timeout <- validate_positive_number(timeout, if_null = Inf, if_na = Inf)
  private$frame   <- caller_env(2L)
  private$trace   <- trace_back()
  
  private$config <- attr(globals, '.jqw_config') # worker for a queue
  
  if (is.null(private$config)) # standalone worker
    private$config <- list(
      globals   = validate_list(globals),
      packages  = validate_character_vector(packages),
      namespace = validate_string(namespace, null_ok = TRUE),
      init      = validate_expression(init, init_subst) )
  
  self$start(wait = wait)
  
  return (invisible(self))
}


w_print <- function (self) {
  cli_text('{self$uid} {.cls {class(self)}} [{self$state}]')
  return (invisible(self))
}


# Create a new R background process.
w_start <- function (self, private, wait, timeout) {
  
  if (private$.state != 'stopped')
    cli_abort('Worker is not stopped')
  
  wait    <- validate_logical(wait)
  timeout <- validate_positive_number(timeout, if_na = private$timeout)
  
  private$.cnd     <- NULL
  private$.is_done <- FALSE
  private$set_state('starting')
  
  private$.tmp <- normalizePath(tempfile('jqw'), winslash = '/', mustWork = FALSE)
  dir.create(private$.tmp)
  
  saveRDS(private$config, private$fp('config.rds'))
  cat(file = private$fp('start.r'), 'jobqueue:::p__start()\n')
  
  cnd <- catch_cnd(zero <- system2(
    command = file.path(R.home('bin'), 'R'),
    args    = shQuote(c(
      '--vanilla', 
      '--slave', 
      '-f', private$fp('start.r'), 
      '--args', private$.tmp) ),
    stdout  = private$fp('stdout.txt'),
    stderr  = private$fp('stderr.txt'), 
    wait    = FALSE ))
  
  
  if (!is_null(cnd) || !identical(zero, 0L)) {
    
    self$stop(error_cnd( # nocov start
      parent  = cnd,
      message = c(
        "can't start Rscript subprocess",
        read_logs(private$.tmp) ))) # nocov end
    
  } else {
    
    # Stop the worker if it spends too long in 'starting' state.
    if (timeout < Inf) {
      msg <- 'worker startup time exceeded {timeout} second{?s}'
      msg <- cli_fmt(cli_text(msg))
      self$on('.next', later(~self$stop(msg, 'timeout'), delay = timeout))
    }
    
    # Keep tabs on the startup progress.
    later(private$poll_startup)
  }
  
  
  if (is_true(wait)) self$wait(timeout = Inf)
  
  return (invisible(self))
}


w_stop <- function (self, private, reason, cls) {
  
  private$.cnd     <- as_cnd(reason, c(cls, 'interrupt'))
  private$.is_done <- TRUE
  
  if (!is_null(job <- private$.job)) {
    private$.job <- NULL
    job$output   <- private$.cnd
  }
  
  private$finalize()
  private$.ps       <- NULL
  private$.tmp      <- NULL
  private$ps_tmp    <- NULL
  private$semaphore <- NULL
  
  private$set_state('stopped')
  
  return (invisible(self))
}


w_restart <- function (self, private, wait, timeout, reason, cls) {
  
  wait    <- validate_logical(wait)
  timeout <- validate_positive_number(timeout, if_na = private$timeout)
  reason  <- validate_string(reason, cnd_ok = TRUE)
  cls     <- validate_character_vector(cls)
  
  self$stop(reason = reason, cls = cls)
  self$start(wait = wait, timeout = timeout)
  
  return (invisible(self))
}


w_run <- function (self, private, job) {
  
  if (!inherits(job, 'Job')) cli_abort('not a Job: {.type {job}}')
  if (!is_null(job$proxy))   cli_abort('cannot run proxied Jobs')
  
  self$wait('idle')
  
  job$state <- 'starting'
  if (job$state != 'starting') cli_abort('Job refused startup')
  job$worker   <- self
  private$.job <- job
  job$on('done', private$job_done)
  
  if (is_null(job$.call))  job$.call  <- caller_env(n = 2L)
  if (is_null(job$.trace)) job$.trace <- trace_back(bottom = job$.call)
  
  # Data to send to the background process.
  request <- list(expr = job$expr, vars = job$vars, cpus = job$cpus)
  saveRDS(request, private$fp('request.rds'))
  
  # Unblock the background process.
  increment_semaphore(private$semaphore)
  
  private$set_state('busy')
  job$state <- 'running'
  later(private$poll_job)
  
  return (invisible(self))
}


# Repeatably check if the Rscript is finished with the job.
w__poll_job <- function (self, private) {
  
  job <- private$.job
  ps  <- private$.ps
  
  if (private$.state != 'busy')    return (NULL)
  if (!inherits(job, 'Job'))       return (NULL)
  if (!inherits(ps,  'ps_handle')) return (NULL)
  
  output_fp <- private$fp('output.rds')
  
  # Crashed?
  if (!ps_is_running(ps)) {
    
    output <- error_cnd(
      call    = job$.call,
      trace   = job$.trace, 
      message = c(
        'worker subprocess terminated unexpectedly',
        read_logs(private$.tmp) ))
    
    private$.job <- NULL
    job$output   <- output
    self$restart(wait = FALSE)
  }
  
  # Finished?
  else if (file.exists(output_fp)) {
    private$.job <- NULL
    job$output   <- readRDS(output_fp)
    unlink(output_fp)
    private$set_state('idle')
  }
  
  else {
    later(private$poll_job, delay = 0.2)
  }
  
  return (NULL)
}


w__poll_startup <- function (self, private) {
  
  if (private$.state != 'starting') return (NULL)
  
  # Import the PID.
  if (is_null(private$.ps) && file.exists(private$fp('ps_info.rds'))) {
      ps_info     <- readRDS(private$fp('ps_info.rds'))
      private$.ps <- try(silent = TRUE, ps::ps_handle(ps_info$pid, ps_info$time))
      private$ps_tmp    <- ps_info$tmpd
      private$semaphore <- ps_info$sem
  }
  
  # Check if alive and for indicator files.
  if (!is_null(ps <- private$.ps)) {
    
    # Crashed?
    fp <- private$fp('error.rds')
    if (!inherits(ps, 'ps_handle') || !ps_is_running(ps) || file.exists(fp)) {
      
      logs <- read_logs(private$.tmp)  # nocov start
      cnd  <- if (file.exists(fp)) readRDS(fp)
      
      if (!is.null(cnd) && !is.null(logs)) { parent <- cnd;  message <- logs }
      else if (!is.null(cnd))              { parent <- cnd;  message <- NULL }
      else if (!is.null(logs))             { parent <- NULL; message <- logs }
      else                                 { parent <- NULL; message <- 'worker startup failed' }
      
      self$stop(error_cnd(call = private$caller_env, parent = parent, message = message))  # nocov end
    }
    
    # Ready?
    else if (file.exists(private$fp('_ready_'))) {
      private$set_state('idle')
    }
    
  }
  
  
  # Still booting.
  if (private$.state == 'starting')
    later(private$poll_startup, delay = 0.2)
  
  return (NULL)
}


# If a running job is interrupted, restart its worker.
w__job_done <- function (self, private, job) {
  if (identical(job$uid, private$.job$uid))
    self$restart(wait = FALSE)
}


