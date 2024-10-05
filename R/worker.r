
#' Evaluates a Job's Expression on a Background Process.
#'
#' @name Worker
#' 
#' @param globals  A list or similar set of values that are added to the 
#'        `.GlobalEnv` of workers.
#' 
#' @param packages  Character vector of package names to load on workers.
#' 
#' @param init  A call or R expression wrapped in curly braces to evaluate on 
#'        each worker just once, immediately after start-up. Will have access 
#'        to any variables defined by `globals` and assets from `packages`. 
#'        Returned value is ignored.
#'        
#' @param hooks  A list of functions to run when the Worker state changes, of 
#'        the form `hooks = list(idle = function (worker) {...}, busy = ~{...})`.
#'        The names of these functions should be `starting`, `idle`, `busy`, 
#'        `stopped`, or `'*'`. `'*'` will be run every time the state changes, 
#'        whereas the others will only be run when the Worker enters that state. 
#'        Duplicate names are allowed.
#'        
#' @param options  Passed to `callr::r_session$new()`
#'        
#' @param job  A [Job] object, as created by `Job$new()`.
#'        
#' @param state  The Worker state that will trigger this function. Typically one of:
#'        \describe{
#'            \item{`'*'` -        }{ Every time the state changes. }
#'            \item{`'.next'` -    }{ Only one time, the next time the state changes. }
#'            \item{`'starting'` - }{ After starting a new `callr::r_session` process. }
#'            \item{`'idle'` -     }{ Waiting on new Jobs to be added. }
#'            \item{`'busy'` -     }{ After a Job starts running. }
#'            \item{`'stopped'` -  }{ After `<Worker>$stop()` is called. }
#'        }
#'        
#' @param func  A function that accepts a Worker object as input. You can call 
#'        `<Worker>$stop()` or edit its values and the changes will be 
#'        persisted (since Workers are reference class objects). You can also 
#'        edit/stop the active or backlogged jobs by modifying `<Worker>$job` 
#'        or `<Worker>$backlog`. Return value is ignored.
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
    #' Creates a background `callr::r_session` process for running [Job]s.
    #' @return A Worker object.
    initialize = function (
        globals  = NULL, 
        packages = NULL, 
        init     = NULL,
        hooks    = NULL,
        options  = callr::r_session_options() ) {
      
      w_initialize(self, private, globals, packages, init, hooks, options)
    },
    
    
    #' @description
    #' Print method for a `Worker`.
    #' @param ... Arguments are not used currently.
    #' @return The Worker, invisibly.
    print = function (...) w_print(self),
    
    #' @description
    #' Starts a new background `callr::r_session` process using the 
    #' configuration previously defined with `Worker$new()`.
    #' @return The Worker, invisibly.
    start = function () w_start(self, private),
    
    #' @description
    #' Stops a Worker by terminating the background `callr::r_session` process 
    #' and calling `<Job>$stop(reason)` on any Jobs currently assigned to this 
    #' Worker.
    #' @return The Worker, invisibly.
    stop = function (reason = 'worker stopped by user') w_stop(self, private, reason),
    
    #' @description
    #' Restarts a Worker by calling `<Worker>$stop(reason)` and 
    #' `<Worker>$start()` in succession.
    #' @return The Worker, invisibly.
    restart = function (reason = 'restarting worker') w_restart(self, reason),
    
    #' @description
    #' Attach a callback function.
    #' @return A function that when called removes this callback from the Worker.
    on = function (state, func) w_on(self, private, state, func),
    
    #' @description
    #' Assigns a Job to this Worker for evaluation on its background 
    #' `callr::r_session` process. Jobs can be assigned even when the Worker is 
    #' in the 'starting' or 'busy' state. A backlog of jobs will be evaluated 
    #' sequentially in the order they were added.
    #' @return This Worker, invisibly.
    run = function (job) w_run(self, private, job)
  ),
  
  private = list(
    
    .hooks       = NULL,
    .options     = NULL,
    .r_session   = NULL,
    .state       = 'stopped',
    .backlog     = list(),
    .loaded      = NULL,
    .uid         = NULL,
    .job         = NULL,
    config_file  = NULL,
    
    set_state    = function (state) w__set_state(self, private, state),
    next_job     = function ()      w__next_job(self, private),
    poll_job     = function ()      w__poll_job(self, private),
    poll_startup = function ()      w__poll_startup(self, private),
    configure    = function ()      w__configure(self, private),
    
    finalize = function () {
      if (!is_null(rs <- private$.r_session))  rs$close()
      if (!is_null(cf <- private$config_file)) unlink(cf)
    }
  ),
  
  active = list(
    
    #' @field hooks
    #' A named list of currently registered callback hooks.
    hooks = function () private$.hooks,
    
    #' @field backlog
    #' A list of Jobs waiting to run on this Worker.
    backlog = function () private$.backlog,
    
    #' @field job
    #' The currently running Job.
    job = function () private$.job,
    
    #' @field r_session
    #' The `callr::r_session` background process interface.
    r_session = function () private$.r_session,
    
    #' @field state
    #' The Worker's state: 'starting', 'idle', 'busy', or 'stopped'.
    state = function () private$.state,
    
    #' @field loaded
    #' A list of global variables and attached functions on this Worker.
    loaded = function () private$.loaded,
    
    #' @field uid
    #' A short string, e.g. 'W11', that uniquely identifies this Worker.
    uid = function () private$.uid
  )
)


w_initialize <- function (self, private, globals, packages, init, hooks, options) {
  
  init_subst <- substitute(init, env = parent.frame())
  
  private$.uid     <- increment_uid('W')
  private$.hooks   <- validate_hooks(hooks, 'WH')
  private$.options <- validate_list(options, if_null = r_session_options())
  
  # See if we have anything for a config file.
  config <- list(
    globals  = validate_list(globals),
    packages = validate_character_vector(packages),
    init     = validate_expression(init, init_subst) )
  
  if (!all(sapply(config, is_null))) {
    private$config_file <- tempfile(fileext = '.rds')
    saveRDS(config, file = private$config_file)
  }
  
  self$start()
  
  return (invisible(self))
}


w_print <- function (self) {
  cli_text('{self$uid} {.cls {class(self)}} [{self$state}]')
  return (invisible(self))
}


w_on <- function (self, private, state, func) {
  
  state <- validate_string(state)
  func  <- validate_function(func, null_ok = FALSE)
  
  uid <- attr(func, '.uid') <- increment_uid('WH')
  private$.hooks %<>% c(setNames(list(func), state))
  
  off <- function () private$.hooks %<>% attr_ne('.uid', uid)
  
  return (invisible(off))
}


# Create a new 'callr::r_session' background process.
w_start <- function (self, private) {
  
  if (private$.state != 'stopped')
    cli_abort('Worker is already {private$.state}.')
  
  private$set_state('starting')
  
  private$.r_session <- r_session$new(
    wait    = FALSE, 
    options = private$.options )
  
  private$poll_startup()
  
  return (invisible(self))
}


w_stop <- function (self, private, reason) {
  
  if (private$.state != 'stopped') {
    
    if (is_function(private$job_off))
      private$job_off()
    
    for (job in c(private$.backlog, private$.job))
      job$output <- list(error = interrupted(reason))
    
    private$finalize()
    
    private$.job        <- NULL
    private$job_off     <- NULL
    private$config_file <- NULL
    private$.backlog    <- list()
    
    private$set_state('stopped')
  }
  
  return (invisible(self))
}


w_restart <- function (self, reason) {
  
  self$stop(reason)
  self$start()
  
  return (invisible(self))
}


# Jobs are internally delayed until the worker is ready for them.
w_run <- function (self, private, job) {
  
  if (!inherits(job, 'Job'))
    cli_abort('`job` must be a Job object, not {.type {job}}.')
  
  private$.backlog %<>% c(job)
  job$worker <- self
  job$state  <- 'dispatched'
  
  private$next_job()
  
  return (invisible(self))
}

w__set_state <- function (self, private, state) {
  
  if (private$.state != state) {
    
    hooks          <- private$.hooks
    private$.hooks <- hooks[names(hooks) != '.next']
    hooks          <- hooks[names(hooks) %in% c('*', '.next', state)]
    
    private$.state <- state
    for (i in seq_along(hooks)) {
      func <- hooks[[i]]
      # uid  <- attr(func, '.uid', exact = TRUE)
      # cli_text('Executing Worker {self$uid} {.val {state}} callback hook {uid}.')
      if (!is_null(formals(func))) { func(self) }
      else if (is.primitive(func)) { func(self) }
      else                         { func()     }
    }
    
  }
  return (invisible(NULL))
}


w__next_job <- function (self, private) {
  
  if (!is_null(private$.job))       return (NULL)
  if (private$.state == 'starting') return (NULL)
  if (private$.state == 'stopped')  return (NULL)
  
  while (length(private$.backlog) > 0) {
    
    job              <- private$.backlog[[1]]
    private$.backlog <- private$.backlog[-1]
    
    if (job$state == 'dispatched') {
      job$state <- 'starting'
      if (job$state == 'starting') {
        private$.job <- job
        break
      }
    }
  }
  
  if (is_null(private$.job)) {
    private$set_state('idle')
    return (NULL)
  }
  
  # If a running job is interrupted, restart its worker. Call job_off() to cancel.
  private$set_state('busy')
  private$job_off <- private$.job$on('done', function (job) { self$restart() })
  
  # Run the user's job on the r_session external process.
  # cli_text('Starting job {private$.job$uid} on {self$uid}.')
  private$.r_session$call(
    args = list(private$.job$expr, private$.job$vars %||% list()), 
    func = function (expr, vars) {
      eval(expr = expr, envir = vars, enclos = .GlobalEnv)
    })
  
  private$.job$state <- 'running'
  later(private$poll_job, 0.5)
}


# Repeatably check if the r_session is finished with the job.
w__poll_job <- function (self, private) {
  
  # cli_text('Polling job {private$.job$uid} on {self$uid}.')
  
  rs <- private$.r_session
  
  if (rs$poll_process(1) == "timeout") {
    
    # The background process is still busy.
    later(private$poll_job, 0.2)
    
  } else {
    # cli_text('Job {private$.job$uid} on {self$uid} is finished.')
    
    # This worker's active job has finished.
    job             <- private$.job
    job_off         <- private$job_off
    private$.job    <- NULL
    private$job_off <- NULL
    
    output <- tryCatch(rs$read(), error = function (e) list(error = e))
    
    # cancel the on('done') trigger that restarts this worker.
    if (is_function(job_off)) job_off()
    
    job$worker <- NULL
    
    # cli_text('Assigning output to job {job$uid}: {.type {output}}.')
    job$output <- output # trigger the job's on('done') triggers.
    
    if (private$.r_session$get_state() == 'idle') {
      private$next_job() # Start the next backlogged job or set worker to 'idle'.
    } else {
      self$restart()
    }
  }
  
  return (NULL)
}


w__poll_startup <- function (self, private) {
  
  rs <- private$.r_session
  
  if (rs$poll_process(1) == "timeout") {
    
    # The background process is still busy.
    later(private$poll_startup, 0.2)
    
  } else {
    
    # Collect the result and other details.
    output <- tryCatch(rs$read(), error = function (e) list(error = e))
    code   <- output[['code']]  # 201 = just booted up; 200 = config finished
    
    if (!is_true(code %in% c(200L, 201L)) || !is_null(output[['error']])) {
      warning('Error when starting r_session worker process:')
      warning(output[['error']] %||% output)
      self$stop('worker startup failed')
    }
    else if (code == 201L) {
      private$configure()
    }
    else if (code == 200L) {
      private$.loaded <- output[['result']]
      private$set_state('idle')
      private$next_job()
    }
    
  }
  
  return (NULL)
}


# r_session finished booting, now add our config to it.
w__configure <- function (self, private) {
  
  private$.r_session$call(
    args = list(config_file = private$config_file), 
    func = function (config_file) {
      
      if (!is.null(config_file)) {
      
        config <- readRDS(config_file)
      
        for (i in seq_along(p <- config[['packages']]))
          require(package = p[[i]], character.only = TRUE)
        
        for (i in seq_along(g <- config[['globals']]))
          assign(x = names(g)[[i]], value = g[[i]], pos = .GlobalEnv)
        
        if (!is.null(i <- config[['init']]))
          eval(expr = i, envir = .GlobalEnv, enclos = .GlobalEnv)
      }
      
      loaded <- list(
        globals  = ls(.GlobalEnv, all.names = TRUE),
        attached = list() )
      
      invisible(lapply(
        X   = rev(intersect(search(), paste0('package:', loadedNamespaces()))), 
        FUN = function (pkg) {
          for (nm in getNamespaceExports(sub('^package:', '', pkg)))
            loaded[['attached']][[nm]] <<- pkg }))
      
      return (loaded)
    })
    
    private$poll_startup()
  
  return (NULL)
}


