
#' A Background Process
#'
#' @name Worker
#' 
#' 
#' @param globals  A list or similar set of values that are added to the 
#'        `.GlobalEnv` of workers.
#' 
#' @param packages  Character vector of package names to load on workers.
#' 
#' @param init  A call or R expression wrapped in curly braces to evaluate on 
#'        each worker just once, immediately after start-up. Will have access 
#'        to variables defined by `globals` and assets from `packages`. 
#'        Returned value is ignored.
#'        
#' @param hooks  A list of functions to run when the Worker state changes, of 
#'        the form `hooks = list(idle = function (worker) {...}, busy = ~{...})`.
#'        The names of these functions should be `starting`, `idle`, `busy`, 
#'        `stopped`, or `'*'`. `'*'` will be run every time the state changes, 
#'        whereas the others will only be run when the Worker enters that state. 
#'        Duplicate names are allowed.
#'        
#' @param job  A [Job] object, as created by `Job$new()`.
#'        
#' @param state  The Worker state that will trigger this function. Typically one of:
#'        \describe{
#'            \item{`'*'` -        }{ Every time the state changes. }
#'            \item{`'.next'` -    }{ Only one time, the next time the state changes. }
#'            \item{`'starting'` - }{ Waiting for the background process to load. }
#'            \item{`'idle'` -     }{ Waiting for Jobs to be `$run()`. }
#'            \item{`'busy'` -     }{ While a Job is running. }
#'            \item{`'stopped'` -  }{ After `<Worker>$stop()` is called. }
#'        }
#'        
#' @param func  A function that accepts a Worker object as input. You can call 
#'        `<Worker>$stop()` or edit its values and the changes will be 
#'        persisted (since Workers are reference class objects).
#'        
#' @param reason  Passed to `<Job>$stop(reason)` for any Jobs currently 
#'        assigned to this Worker.
#'
#' @export
#' 


Worker <- R6Class(
  classname    = "Worker",
  cloneable    = FALSE,
  lock_objects = FALSE,
  
  public = list(
    
    #' @description
    #' Creates a background Rscript process for running [Job]s.
    #' @return A Worker object.
    initialize = function (
        globals  = NULL, 
        packages = NULL, 
        init     = NULL,
        hooks    = NULL ) {
      
      w_initialize(self, private, globals, packages, init, hooks)
    },
    
    
    #' @description
    #' Print method for a `Worker`.
    #' @param ... Arguments are not used currently.
    #' @return The Worker, invisibly.
    print = function (...) w_print(self),
    
    #' @description
    #' Starts a new background Rscript process using the 
    #' configuration previously defined with `Worker$new()`.
    #' @return The Worker, invisibly.
    start = function () w_start(self, private),
    
    #' @description
    #' Stops a Worker by terminating the background Rscript process and calling 
    #' `<Job>$stop(reason)` on any Jobs currently assigned to this Worker.
    #' @return The Worker, invisibly.
    stop = function (reason = 'worker stopped by user')
      w_stop(self, private, reason),
    
    #' @description
    #' Restarts a Worker by calling `<Worker>$stop(reason)` and 
    #' `<Worker>$start()` in succession.
    #' @return The Worker, invisibly.
    restart = function (reason = 'restarting worker') 
      w_restart(self, private, reason),
    
    #' @description
    #' Attach a callback function.
    #' @return A function that when called removes this callback from the Worker.
    on = function (state, func) u_on(self, private, 'WH', state, func),
    
    #' @description
    #' Blocks until the Worker enters the given state.
    #' @return This Worker, invisibly.
    wait = function (state = 'idle') u_wait(self, private, state),
    
    #' @description
    #' Assigns a Job to this Worker for evaluation on its background 
    #' Rscript process. Worker must be in `'idle'` state.
    #' @return This Worker, invisibly.
    run = function (job) w_run(self, private, job)
  ),
  
  private = list(
    
    .hooks     = NULL,
    .state     = 'stopped',
    .loaded    = NULL,
    .uid       = NULL,
    .job       = NULL,
    .ps        = NULL,
    .wd        = NULL,
    caller_env = NULL,
    lock       = NULL,
    
    set_state    = function (state) u__set_state(self, private, state),
    poll_job     = function ()      w__poll_job(self, private),
    poll_startup = function ()      w__poll_startup(self, private),
    configure    = function ()      w__configure(self, private),
    job_done     = function (job)   w__job_done(self, private, job),
    fp           = function (...)   file.path(private$.wd, paste0(...)),
    
    finalize = function () {
      if (!is_null(ps  <- private$.ps))  ps_kill(ps)
      if (!is_null(lck <- private$lock)) unlock(lck)
      if (!is_null(wd  <- private$.wd))  unlink(wd, recursive = TRUE)
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
    #' The `ps::ps_handle()` for the background process.
    ps = function () private$.ps,
    
    #' @field state
    #' The Worker's state: 'starting', 'idle', 'busy', or 'stopped'.
    state = function () private$.state,
    
    #' @field loaded
    #' A list of global variables and attached functions on this Worker.
    loaded = function () w_loaded(self, private),
    
    #' @field uid
    #' A short string, e.g. 'W11', that uniquely identifies this Worker.
    uid = function () private$.uid,
    
    #' @field wd
    #' The Worker's working directory.
    wd = function () private$.wd
  )
)


w_initialize <- function (self, private, globals, packages, init, hooks) {
  
  init_subst <- substitute(init, env = parent.frame())
  
  private$.uid       <- increment_uid('W')
  private$.wd        <- working_dir(private$.uid)
  private$.hooks     <- validate_hooks(hooks, 'WH')
  private$caller_env <- caller_env(2L)
  
  saveRDS(file = private$fp('config.rds'), list(
    globals  = validate_list(globals, if_null = NULL),
    packages = validate_character_vector(packages),
    init     = validate_expression(init, init_subst) ))
  
  self$start()
  
  return (invisible(self))
}


w_print <- function (self) {
  cli_text('{self$uid} {.cls {class(self)}} [{self$state}]')
  return (invisible(self))
}


# Create a new Rscript background process.
w_start <- function (self, private) {
  
  if (private$.state != 'stopped')
    cli_abort('Worker is already {private$.state}.')
  
  private$set_state('starting')
  private$lock <- lock(private$fp('ready.lock'))
  
  cnd <- catch_cnd(system2(
    command = 'Rscript', 
    args    = c('--vanilla', '-e', 'jobqueue:::p__start()', shQuote(private$.wd)), 
    wait    = FALSE, 
    stdout  = private$fp('stdout.txt'), 
    stderr  = private$fp('stderr.txt') ))
  
  if (!is_null(cnd))
    self$stop(error_cnd(
      parent  = cnd,
      message = c(
        "can't start Rscript subprocess",
        read_logs(private$.wd) )))
  
  private$poll_startup()
  
  return (invisible(self))
}


w_stop <- function (self, private, reason) {
    
  output <- interruptCondition(reason)
  
  if (!is_null(job <- private$.job)) {
    private$.job <- NULL
    job$output   <- output
  }
  
  if (!is_null(ps <- private$.ps)) {
    if (ps_is_running(ps)) ps_kill(ps)
    private$.ps <- NULL
  }
  
  if (!is_null(private$lock)) {
    unlock(private$lock)
    private$lock <- NULL
  }
  
  files <- list.files(private$.wd)
  files <- setdiff('config.rds', files)
  unlink(private$fp(files))
  
  private$set_state('stopped')
  
  if (inherits(reason, 'condition'))
    cnd_signal(reason)
  
  return (invisible(self))
}


w_restart <- function (self, private, reason) {
  self$stop(reason)
  self$start()
  return (invisible(self))
}


w_loaded <- function (self, private) {
  if (is_null(private$.loaded)) {
    fp <- file.path(private$.wd, 'loaded.rds')
    private$.loaded <- readRDS(fp)
  }
  return (private$.loaded)
}


w_run <- function (self, private, job) {
  
  if (!inherits(job, 'Job')) cli_abort('not a Job: {.type {job}}')
  if (self$state != 'idle')  cli_abort('Worker not idle')
  if (!is_null(job$proxy))   cli_abort('cannot run proxied Jobs')
  
  job$state <- 'starting'
  if (job$state != 'starting') cli_abort('Job refused startup')
  
  private$set_state('busy')
  
  private$.job <- job
  job$worker   <- self
  if (is_null(job$caller_env))
    job$caller_env <- caller_env(2L)
  
  # Data to send to the background process.
  saveRDS(
    file   = private$fp('request.rds'), 
    object = list(uid = job$uid, expr = job$expr, vars = job$vars) )
  
  # Unblock / re-block the background process.
  old_lock     <- private$lock
  private$lock <- lock(private$fp(job$uid, '.lock'))
  unlock(old_lock)
  
  job$state <- 'running'
  job$on('done', private$job_done)
  
  later(private$poll_job, 0.2)
  
  return (invisible(self))
}


# Repeatably check if the Rscript is finished with the job.
w__poll_job <- function (self, private) {
  
  job <- private$.job
  
  if (private$.state == 'busy' && !is_null(job)) {
    
    # Crashed?
    if (!ps_is_running(ps <- private$.ps)) {
      
      output <- error_cnd(
        call    = job$caller_env, 
        message = c(
          'worker subprocess terminated unexpectedly',
          read_logs(private$.wd) ))
      
      private$.job <- NULL
      job$output   <- output
      self$restart()
    }
    
    # Finished?
    else if (file.exists(fp <- private$fp('output.rds'))) {
      private$.job <- NULL
      job$output   <- readRDS(fp)
      private$set_state('idle')
    }
    
    # Running?
    else {
      later(private$poll_job, 0.2)
    }
    
  }
  
  return (NULL)
}


w__poll_startup <- function (self, private) {
  
  if (private$.state == 'starting') {
    
    
    # Find PID
    if (is_null(private$.ps))
      if (file.exists(fp <- private$fp('pid.txt')))
        private$.ps <- ps_handle(pid = as.integer(readLines(fp)))
    
    # No PID yet
    if (is_null(private$.ps)) {
      later(private$poll_startup, 0.2)
    }
    
    # Crashed?
    else if (!ps_is_running(private$.ps)) {
      
      parent_fp <- private$fp('error.rds')
      parent    <- if (file.exists(parent_fp)) readRDS(parent_fp)
        
      self$stop(error_cnd(
        call    = private$caller_env, 
        parent  = parent,
        message = c(
          'worker startup failed',
          read_logs(private$.wd) )))
    }
    
    # Ready?
    else if (file.exists(private$fp('_ready_'))) {
      private$set_state('idle')
    }
    
    # Starting?
    else {
      later(private$poll_startup, 0.2)
    }
  
  }
  
  return (NULL)
}


# If a running job is interrupted, restart its worker.
w__job_done <- function (self, private, job) {
  if (identical(job$uid, private$.job$uid))
    self$restart()
}
