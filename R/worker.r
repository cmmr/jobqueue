
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
    semaphore  = NULL,
    
    set_state    = function (state) u__set_state(self, private, state),
    poll_job     = function ()      w__poll_job(self, private),
    poll_startup = function ()      w__poll_startup(self, private),
    job_done     = function (job)   w__job_done(self, private, job),
    fp           = function (...)   file.path(private$.wd, paste0(...)),
    
    finalize = function () {
      if (!is_null(private$.ps))       ps_kill(private$.ps)
      if (!is_null(private$semaphore)) remove_semaphore(private$semaphore)
      if (!is_null(private$.wd))       unlink(private$.wd, recursive = TRUE)
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
    #' The Worker's state: 'starting', 'idle', 'busy', or 'stopped'.
    state = function () private$.state,
    
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
  
  private$semaphore <- create_semaphore()
  saveRDS(private$semaphore, private$fp('semaphore.rds'))
  
  cnd <- catch_cnd(system2(
    command = 'Rscript',
    args    = shQuote(c('--vanilla', '-e', 'jobqueue:::p__start()', private$.wd)),
    stdout  = private$fp('stdout.txt'),
    stderr  = private$fp('stderr.txt'), 
    wait    = FALSE ))
  
  if (!is_null(cnd))
    self$stop(error_cnd(
      parent  = cnd,
      message = c(
        "can't start Rscript subprocess",
        read_logs(private$.wd) )))
  
  later(private$poll_startup)
  
  return (invisible(self))
}


w_stop <- function (self, private, reason) {
  
  if (!is_null(job <- private$.job)) {
    private$.job <- NULL
    job$output   <- interruptCondition(reason)
  }
  
  if (!is_null(ps <- private$.ps)) {
    if (ps_is_running(ps)) ps_kill(ps)
    private$.ps <- NULL
  }
  
  if (!is_null(private$semaphore)) {
    remove_semaphore(private$semaphore)
    private$semaphore <- NULL
  }
  
  files <- list.files(private$.wd)
  files <- setdiff(files, 'config.rds')
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
  saveRDS(
    file   = private$fp('request.rds'), 
    object = list(expr = job$expr, vars = job$vars) )
  
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
        read_logs(private$.wd) ))
    
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
  
  # Crashed?
  if (file.exists(fp <- private$fp('error.rds'))) {
    self$stop(error_cnd(
      call    = private$caller_env, 
      parent  = if (file.exists(fp)) readRDS(fp),
      message = c(
        'worker startup failed: error occured',
        read_logs(private$.wd) )))
  }
  
  # Ready?
  else if (file.exists(fp <- private$fp('ps_info.rds'))) {
    cnd <- catch_cnd({
      ps_info     <- readRDS(fp)
      private$.ps <- ps::ps_handle(ps_info$pid, ps_info$time) })
    
    if (!is_null(cnd) || !ps_is_running(private$.ps)) {
      self$stop(error_cnd(
        call    = private$caller_env, 
        parent  = cnd,
        message = c(
          'worker startup failed: process not running',
          read_logs(private$.wd) )))
      
    } else {
      private$set_state('idle')
    }
    
  }
  
  # Booting?
  else if (private$.state == 'starting') {
    later(private$poll_startup, delay = 0.2)
  }
  
  return (NULL)
}


# If a running job is interrupted, restart its worker.
w__job_done <- function (self, private, job) {
  if (identical(job$uid, private$.job$uid))
    self$restart()
}
