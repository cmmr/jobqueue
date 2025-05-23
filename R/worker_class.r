
#' A Background Process (R6 Class)
#'
#' @description
#' 
#' Where job expressions are evaluated.
#' 
#' 
#' @param globals  A named list of variables that all `<job>$expr`s will have 
#'        access to. Alternatively, an object that can be coerced to a named 
#'        list with `as.list()`, e.g. named vector, data.frame, or environment.
#' 
#' @param packages  Character vector of package names to load on 
#'        [`workers`][worker_class].
#' 
#' @param namespace  The name of a package to attach to the 
#'        [`worker's`][worker_class] environment.
#' 
#' @param init  A call or R expression wrapped in curly braces to evaluate on 
#'        each [`worker`][worker_class] just once, immediately after start-up. 
#'        Will have access to variables defined by `globals` and assets from 
#'        `packages` and `namespace`. Returned value is ignored.
#'        
#' @param hooks  A named list of functions to run when the 
#'        [`worker`][worker_class] state changes, of the form 
#'        `hooks = list(idle = function (worker) {...})`. Names of 
#'        [`worker`][worker_class] hooks are typically `starting`, `idle`, 
#'        `busy`, `stopped`, or `'*'` (duplicates okay). 
#'        See `vignette('hooks')`.
#'        
#' @param wait  If `TRUE`, blocks until the [`worker`][worker_class] is 'idle'. 
#'        If `FALSE`, the [`worker`][worker_class] object is returned in the 
#'        'starting' state.
#'        
#' @param timeout  How long to wait for the [`worker`][worker_class] to finish 
#'        starting (in seconds). If `NA`, defaults to the `worker_class$new()` 
#'        argument.
#'        
#' @param job  A [`job`][job_class] object, as created by `job_class$new()`.
#'        
#' @param state
#' The name of a [`worker`][worker_class] state. Typically one of:
#' 
#' * `'*'` -        Every time the state changes.
#' * `'.next'` -    Only one time, the next time the state changes.
#' * `'starting'` - Waiting for the background process to load.
#' * `'idle'` -     Waiting for [`jobs`][job_class] to be `$run()`.
#' * `'busy'` -     While a [`job`][job_class] is running.
#' * `'stopped'` -  After `<worker>$stop()` is called.
#' 
#'        
#' @param func  A function that accepts a [`worker`][worker_class] object as 
#'        input. You can call `<worker>$stop()` and other `<worker>$` methods.
#'        
#' @param reason  Passed to `<job>$stop()` for any [`jobs`][job_class] 
#'        currently managed by this [`worker`][worker_class].
#'        
#' @param cls  Passed to `<job>$stop()` for any [`jobs`][job_class] currently 
#'        managed by this [`worker`][worker_class].
#' 
#' @export
#' 


worker_class <- R6Class(
  classname    = "worker",
  cloneable    = FALSE,
  lock_objects = FALSE,
  
  public = list(
    
    #' @description
    #' Creates a background R process for running [`jobs`][job_class].
    #' @return A [`worker`][worker_class] object.
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
    #' Print method for a [`worker`][worker_class].
    #' @param ... Arguments are not used currently.
    #' @return The [`worker`][worker_class], invisibly.
    print = function (...) 
      w_print(self),
    
    #' @description
    #' Restarts a stopped [`worker`][worker_class].
    #' @return The [`worker`][worker_class], invisibly.
    start = function (wait = TRUE, timeout = NA) 
      w_start(self, private, wait, timeout),
    
    #' @description
    #' Stops a [`worker`][worker_class] by terminating the background process 
    #' and calling `<job>$stop(reason)` on any [`jobs`][job_class] currently 
    #' assigned to this [`worker`][worker_class].
    #' @return The [`worker`][worker_class], invisibly.
    stop = function (reason = 'worker stopped by user', cls = NULL)
      w_stop(self, private, reason, cls),
    
    #' @description
    #' Restarts a [`worker`][worker_class] by calling `<worker>$stop(reason)` 
    #' and `<worker>$start()` in succession.
    #' @return The [`worker`][worker_class], invisibly.
    restart = function (wait = TRUE, timeout = NA, reason = 'restarting worker', cls = NULL) 
      w_restart(self, private, wait, timeout, reason, cls),
    
    #' @description
    #' Attach a callback function to execute when the [`worker`][worker_class] 
    #' enters `state`.
    #' @return A function that when called removes this callback from the 
    #' [`worker`][worker_class].
    on = function (state, func) 
      u_on(self, private, 'WH', state, func),
    
    #' @description
    #' Blocks until the [`worker`][worker_class] enters the given state.
    #' @param timeout Stop the [`worker`][worker_class] if it takes longer than 
    #'        this number of seconds.
    #' @param signal Raise an error if encountered (will also be recorded in 
    #'        `<worker>$cnd`).
    #' @return This [`worker`][worker_class], invisibly.
    wait = function (state = 'idle', timeout = Inf, signal = TRUE) 
      u_wait(self, private, state, timeout, signal),
    
    #' @description
    #' Assigns a [`job`][job_class] to this [`worker`][worker_class] for 
    #' evaluation on the background process.
    #' @return This [`worker`][worker_class], invisibly.
    run = function (job) 
      w_run(self, private, job)
  ),
  
  private = list(
    
    .hooks   = NULL,
    .state   = 'stopped',
    .is_done = FALSE,
    .job     = NULL,
    .ps      = NULL, # ps::ps_handle object
    .cnd     = NULL,
    frame    = NULL,
    trace    = NULL,
    w_dir    = NULL,
    p_dir    = NULL,
    sem      = NULL, # semaphore for blocking eval loop
    config   = NULL, # path to config.rds
    
    set_state    = function (state) u__set_state(self, private, state),
    poll_job     = function ()      w__poll_job(self, private),
    poll_startup = function ()      w__poll_startup(self, private),
    job_done     = function (job)   w__job_done(self, private, job),
    
    finalize = function () {
      if (!is_null(ps  <- private$.ps))   ps_kill(ps)
      if (!is_null(sem <- private$sem))   sem$remove()
      if (!is_null(dir <- private$p_dir)) dir_delete(dir)
    }
    
  ),
  
  active = list(
    
    #' @field hooks
    #' A named list of currently registered callback hooks.
    hooks = function () private$.hooks,
    
    #' @field job
    #' The currently running [`job`][job_class].
    job = function () private$.job,
    
    #' @field ps
    #' The `ps::ps_handle()` object for the background process.
    ps = function () private$.ps,
    
    #' @field state
    #' The [`worker's`][worker_class] state: `'starting'`, `'idle'`, `'busy'`, 
    #' or `'stopped'`.
    state = function () private$.state,
    
    #' @field uid
    #' A short string, e.g. `'W11'`, that uniquely identifies this 
    #' [`worker`][worker_class].
    uid = function () basename(private$w_dir),
    
    #' @field tmp
    #' The [`worker's`][worker_class] temporary directory.
    tmp = function () private$w_dir,
    
    #' @field cnd
    #' The error that caused the [`worker`][worker_class] to stop.
    cnd = function () private$.cnd
  )
)


w_initialize <- function (self, private, globals, packages, namespace, init, hooks, wait, timeout) {
  
  init_subst <- substitute(init, env = parent.frame())
  
  wait <- validate_logical(wait)
  
  private$.hooks  <- validate_hooks(hooks, 'WH')
  private$timeout <- validate_positive_number(timeout, if_null = Inf, if_na = Inf)
  private$frame   <- caller_env(2L)
  private$trace   <- trace_back()
  
  # This worker's working directory.
  if (inherits(globals, 'jobqueue')) {
    private$w_dir  <- dir_create(globals$tmp, 'W')
    private$config <- file.path(globals$tmp, 'config.rds')
  }
  else {
    private$w_dir  <- dir_create(c(ENV$jq_dir, 'Q0'), 'W')
    private$config <- file.path(private$w_dir, 'config.rds')
    
    saveRDS(
      file   = private$config, 
      object = list(
        'globals'   = validate_list(globals),
        'packages'  = validate_character_vector(packages),
        'namespace' = validate_string(namespace, null_ok = TRUE),
        'init'      = validate_expression(init, init_subst) ))
  }
  
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
    cli_abort('worker is not stopped')
  
  wait    <- validate_logical(wait)
  timeout <- validate_positive_number(timeout, if_na = private$timeout)
  
  private$.cnd     <- NULL
  private$.is_done <- FALSE
  private$set_state('starting')
  
  # Disposable directory for fresh worker process
  private$p_dir <- dir_create(private$w_dir, 'P')
  
  # Once per R session, spawn a monitoring process
  p__spawn_monitor()
  
  # A script that calls p__start.
  cmdfile <- file.path(private$p_dir, 'cmds.r')
  writeLines(con = cmdfile, c(
    paste0('ppid   <- ', shQuote(ENV$pid)),
    paste0('mqid   <- ', shQuote(ENV$mqid)),
    paste0('p_dir  <- ', shQuote(private$p_dir)),
    paste0('config <- ', shQuote(private$config)),
    paste0('jobqueue:::p__start(ppid, mqid, p_dir, config)') ))
  
  # Start the worker's subprocess.
  system2(
    command = file.path(R.home('bin'), 'Rscript'),
    args    = c('--vanilla', shQuote(cmdfile)),
    wait    = FALSE,
    stdout  = file.path(private$p_dir, 'stdout.txt'),
    stderr  = file.path(private$p_dir, 'stderr.txt') )
  
  
  # Stop the worker if it spends too long in 'starting' state.
  if (timeout < Inf) {
    msg <- 'worker startup time exceeded {timeout} second{?s}'
    msg <- cli_fmt(cli_text(msg))
    self$on('.next', later(~self$stop(msg, 'timeout'), delay = timeout))
  }
  
  # Keep tabs on the startup progress.
  later(private$poll_startup)
  if (is_true(wait))
    self$wait(timeout = Inf)
  
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
  
  private$.ps   <- NULL
  private$p_dir <- NULL
  private$sem   <- NULL
  
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
  
  if (!inherits(job, 'job')) cli_abort('not a job: {.type {job}}')
  if (!is_null(job$proxy))   cli_abort('cannot run proxied jobs')
  
  self$wait('idle')
  
  job$state <- 'starting'
  if (job$state != 'starting') cli_abort('job refused startup')
  job$worker   <- self
  private$.job <- job
  job$on('done', private$job_done)
  
  if (is_null(job$.call))  job$.call  <- caller_env(n = 2L)
  if (is_null(job$.trace)) job$.trace <- trace_back(bottom = job$.call)
  
  # Data to send to the background process.
  request <- list(expr = job$expr, vars = job$vars, cpus = job$cpus)
  saveRDS(request, file.path(private$p_dir, 'request.rds'))
  
  # Unblock the background process.
  private$sem$post()
  
  private$set_state('busy')
  job$state <- 'running'
  later(private$poll_job)
  
  return (invisible(self))
}


# Repeatably check if the Rscript is finished with the job.
w__poll_job <- function (self, private) {
  
  job   <- private$.job
  ps    <- private$.ps
  p_dir <- private$p_dir
  
  if (private$.state != 'busy')    return (NULL)
  if (!inherits(job, 'job'))       return (NULL)
  if (!inherits(ps,  'ps_handle')) return (NULL)
  
  # Crashed?
  if (!ps::ps_is_running(ps)) {
    
    output <- error_cnd(
      call    = job$.call,
      trace   = job$.trace, 
      message = c(
        'worker subprocess terminated unexpectedly',
        read_logs(p_dir) ))
    
    private$.job <- NULL
    job$output   <- output
    self$restart(wait = FALSE)
  }
  
  # Finished?
  else if (file_exists(p_dir, 'done')) {
    
    private$.job <- NULL
    job$output   <- readRDS(file.path(p_dir, 'output.rds'))
    dir_cleanup(p_dir)
    private$set_state('idle')
  }
  
  else {
    later(private$poll_job, delay = 0.2)
  }
  
  return (NULL)
}


w__poll_startup <- function (self, private) {
  
  if (private$.state != 'starting') return (NULL)
  
  p_dir <- private$p_dir
  
  # Look for 'pid_' file
  if (is_null(private$.ps))
    private$.ps <- dir_ps(p_dir)
  
  
  # Check if alive and for indicator files.
  if (inherits(private$.ps, 'ps_handle')) {
    
    ps <- private$.ps
    
    # Crashed?
    if (!inherits(ps, 'ps_handle') || !ps::ps_is_running(ps)) {
      message <- read_logs(p_dir)  # nocov start
      if (is.null(message)) message <- 'worker startup failed'
      self$stop(error_cnd(call = private$caller_env, message = message))  # nocov end
    }
    
    # Ready?
    else if (file_exists(p_dir, 'ready')) {
      private$sem <- dir_sem(p_dir)
      dir_cleanup(p_dir)
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
