
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
#' @param init  A call or R expression wrapped in curly braces to evaluate on 
#'        each worker just once, immediately after start-up. Will have access 
#'        to variables defined by `globals` and assets from `packages`. 
#'        Returned value is ignored.
#'        
#' @param hooks  A named list of functions to run when the Worker state 
#'        changes, of the form `hooks = list(idle = function (worker) {...})`.
#'        Names of worker hooks are typically `starting`, `idle`, `busy`, 
#'        `stopped`, or `'*'` (duplicates okay). See `vignette('hooks')`.
#'        
#' @param wait  If `TRUE`, blocks until the Worker is 'idle'. If `FALSE`, the 
#'        Worker object is returned in the 'starting' state. If a number, blocks 
#'        at most that number of seconds before returning either an 'idle' or 
#'        'stopped' Worker.
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
        globals  = NULL, 
        packages = NULL, 
        init     = NULL,
        hooks    = NULL,
        wait     = TRUE ) {
      
      w_initialize(self, private, globals, packages, init, hooks, wait)
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
    start = function () 
      w_start(self, private),
    
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
    restart = function (reason = 'restarting worker') 
      w_restart(self, private, reason),
    
    #' @description
    #' Attach a callback function to execute when the Worker enters `state`.
    #' @return A function that when called removes this callback from the Worker.
    on = function (state, func) 
      u_on(self, private, 'WH', state, func),
    
    #' @description
    #' Blocks until the Worker enters the given state.
    #' @param timeout Stop the Worker if it takes longer than this number of seconds, or `NULL`.
    #' @return This Worker, invisibly.
    wait = function (state = 'idle', timeout = NULL) 
      u_wait(self, private, state, timeout),
    
    #' @description
    #' Assigns a Job to this Worker for evaluation on the background 
    #' process. *Worker must be in the `'idle'` state.*
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
    .reason    = NULL,
    ps_tmp     = NULL, # child's tempdir()
    old_dirs   = NULL,
    semaphore  = NULL,
    caller_env = NULL,
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
    
    #' @field reason
    #' Why the Worker was stopped.
    reason = function () private$.reason,
    
    #' @field state
    #' The Worker's state: `'starting'`, `'idle'`, `'busy'`, or `'stopped'`.
    state = function () private$.state,
    
    #' @field uid
    #' A short string, e.g. `'W11'`, that uniquely identifies this Worker.
    uid = function () private$.uid,
    
    #' @field tmp
    #' The Worker's temporary directory.
    tmp = function () private$.tmp
  )
)


w_initialize <- function (self, private, globals, packages, init, hooks, wait) {
  
  init_subst <- substitute(init, env = parent.frame())
  
  private$.uid       <- increment_uid('W')
  private$.hooks     <- validate_hooks(hooks, 'WH')
  private$caller_env <- caller_env(2L)
  
  private$config <- attr(globals, '.jqw_config') # worker for a queue
  
  if (is.null(private$config)) # standalone worker
    private$config <- list(
      globals  = validate_list(globals),
      packages = validate_character_vector(packages),
      init     = validate_expression(init, init_subst) )
  
  self$start()
  
  if (is_true(wait))         self$wait()
  else if (is.numeric(wait)) self$wait(timeout = wait)
  
  if (is_true(private$.is_done))
    cli_abort('Unable to start Worker')
  
  return (invisible(self))
}


w_print <- function (self) {
  cli_text('{self$uid} {.cls {class(self)}} [{self$state}]')
  return (invisible(self))
}


# Create a new R background process.
w_start <- function (self, private) {
  
  if (private$.state != 'stopped')
    cli_abort('Worker is already {private$.state}.')
  
  private$.is_done <- FALSE
  private$set_state('starting')
  private$.reason <- NULL
  
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
    timeout <- getOption('jobqueue.worker_timeout', default = 5L)
    timeout <- validate_positive_number(timeout)
    if (!is.null(timeout)) {
      msg <- 'worker startup time exceeded {timeout} second{?s}'
      msg <- cli_fmt(cli_text(msg))
      self$on('.next', later(~self$stop(msg, 'timeout'), delay = timeout))
    }
    
    # Keep tabs on the startup progress.
    later(private$poll_startup)
  }
  
  
  return (invisible(self))
}


w_stop <- function (self, private, reason, cls) {
  
  private$.reason  <- reason
  private$.is_done <- TRUE
  
  if (!is_null(job <- private$.job)) {
    private$.job <- NULL
    job$output   <- interrupt_cnd(reason, cls)
  }
  
  private$finalize()
  private$.ps       <- NULL
  private$.tmp      <- NULL
  private$ps_tmp    <- NULL
  private$semaphore <- NULL
  
  private$set_state('stopped')
  
  return (invisible(self))
}


w_restart <- function (self, private, reason) {
  self$stop(reason = reason)
  self$start()
  return (invisible(self))
}


w_run <- function (self, private, job) {
  
  if (!inherits(job, 'Job')) cli_abort('not a Job: {.type {job}}')
  if (self$state != 'idle')  cli_abort('Worker not idle')
  if (!is_null(job$proxy))   cli_abort('cannot run proxied Jobs')
  
  job$state <- 'starting'
  if (job$state != 'starting') cli_abort('Job refused startup')
  job$worker   <- self
  private$.job <- job
  job$on('done', private$job_done)
  
  if (is_null(job$caller_env))
    job$caller_env <- caller_env(2L)
  
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
      call    = job$caller_env, 
      message = c(
        'worker subprocess terminated unexpectedly',
        read_logs(private$.tmp) ))
    
    private$.job <- NULL
    job$output   <- output
    self$restart()
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
      
      self$stop(error_cnd(
        call    = private$caller_env, 
        parent  = if (file.exists(fp)) readRDS(fp),
        message = c(
          'worker startup failed',
          read_logs(private$.tmp) )))
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
    self$restart()
}


