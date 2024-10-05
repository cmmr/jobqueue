
#' Interruptible, Asynchronous Background Jobs.
#'
#' @name Queue
#'
#' @description
#' Submit your jobs to a Queue and it will run them on a background R 
#' process when one becomes available. Jobs are launched in the same order as 
#' they are submitted, but may finish in a different order based on each job's 
#' run time. Treat the returned Job object as a promise for asynchronous 
#' downstream processing of the job's result.
#' 
#' Starts R sessions that run in the background. Calls to `run()` evaluate R
#' functions on these background sessions. `run()` returns a [Job]
#' object which can be passed to `then()` to schedule work to be done on the
#' result, when it is ready.
#'
#' The [Job] object also has a `$stop()` element which can be called to return 
#' a result immediately. If the job was actively running in a background R 
#' session, that process is killed and a new process is started to take its 
#' place.
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
#' @param max_cpus  Total number of CPU cores that can be reserved all running 
#'        Jobs via their combined `cpus` arguments. Does not enforce limits on 
#'        actual CPU utilization.
#'        
#' @param workers  How many background [Worker] processes to start. Set to more 
#'        than `max_cpus` to enable interrupted workers to be quickly swapped 
#'        out with standby Workers while a replacement Worker boots up.
#'        
#' @param options  Passed to `callr::r_session$new()`
#'        
#' @param job  A [Job] object, as created by `Job$new()`.
#'        
#' @param start  Should a Job be submitted to the Queue (`start = TRUE`) or 
#'        just created (`start = FALSE`)?
#'        
#' @param stop_id  If an existing [Job] in the Queue has the same `stop_id`,  
#'        that Job will be stopped and return an 'interrupt' condition object 
#'        as its result. `stop_id` can also be a `function (job)` that returns 
#'        the `stop_id` to assign to a given Job. A `stop_id` of `NULL` 
#'        disables this feature.
#'                 
#' @param copy_id  If an existing [Job] in the Queue has the same `copy_id`, 
#'        the newly submitted Job will become a "proxy" for that earlier Job, 
#'        returning whatever result the earlier Job returns. `copy_id` can also 
#'        be a `function (job)` that returns the `copy_id` to assign to a given 
#'        Job. A `copy_id` of `NULL` disables this feature.
#' 
#' @param scan  Should additional variables be added to `vars` based on 
#'        scanning `expr` for missing global variables? By default, 
#'        `scan = is.null(vars)`, meaning if you set `vars = list()` then no 
#'        scan is done. Set `scan = TRUE` to always scan, `scan = FALSE` to 
#'        never scan, and `scan = <an environment-like object>` to look for 
#'        globals there. When scanning, the worker's environment is taken into 
#'        account, and globals on the worker are favored over globals locally. 
#'        `vars` defined by the user are always left untouched.
#' 
#' @param ignore  A character vector of variable names that should NOT be added 
#'        to `vars` by `scan`.
#'        
#' @param reason  Passed to `<Job>$stop(reason)` for any Jobs currently 
#'        managed by this Queue.
#'
#' @export
#' 

Queue <- R6Class(
  classname    = "Queue",
  cloneable    = FALSE,
  lock_objects = FALSE,

  public = list(

    #' @description
    #' Creates n `workers` background processes for handling `$run()` and 
    #' `$submit()` calls. These workers are initialized according to the 
    #' `globals`, `packages`, `init`, and `options` arguments. The Queue will 
    #' not use more than `max_cpus` at once, assuming the `cpus` argument is 
    #' properly set for each Job.
    #'
    #' @param tmax,hooks,reformat,stop_id,copy_id
    #'        Defaults for this Queue's `$run()` method. Here, `stop_id` and 
    #'        `copy_id` must be a `function (job)` or `NULL`.
    #'
    #' @return A `Queue` object.
    initialize = function (
        globals   = NULL,
        packages  = NULL,
        init      = NULL,
        max_cpus  = availableCores(omit = 1L),
        workers   = ceiling(max_cpus * 1.2),
        options   = r_session_options(),
        tmax      = NULL,
        hooks     = NULL,
        reformat  = TRUE,
        stop_id   = NULL,
        copy_id   = NULL ) {
       
      q_initialize(
        self, private, 
        globals, packages, init, max_cpus, workers, options, 
        tmax, hooks, reformat, stop_id, copy_id )
    },
    
    
    #' @description
    #' Print method for a `Queue`.
    #' @param ... Arguments are not used currently.
    print = function (...) q_print(self, private),
    
    
    #' @description
    #' Creates a Job object and submits it to the queue for running on a 
    #' background process. Here, the default `NA` value will use the value set 
    #' by `Queue$new()`.
    #'
    #' @return The new [Job] object.
    run = function (
        expr, 
        vars     = NULL, 
        scan     = is.null(vars), 
        ignore   = NULL, 
        tmax     = NA, 
        hooks    = NA, 
        reformat = NA, 
        cpus     = 1L, 
        stop_id  = NA, 
        copy_id  = NA, 
        start    = TRUE ) {
      
      q_run(
        self     = self, 
        private  = private, 
        expr     = expr, 
        vars     = vars, 
        scan     = scan, 
        ignore   = ignore, 
        tmax     = tmax, 
        hooks    = hooks, 
        reformat = reformat, 
        cpus     = cpus, 
        stop_id  = stop_id, 
        copy_id  = copy_id, 
        start    = start )
    },
    
    
    #' @description
    #' Adds a Job to the Queue for running on a background process.
    #' @return This Queue, invisibly.
    submit = function (job)
      q_submit(self, private, job),


    #' @description
    #' Stop all jobs and workers.
    #' @return This Queue, invisibly.
    shutdown = function (reason = 'job queue shut down by user') {
      q_shutdown(self, private, reason)
    }
  ),


  private = list(
    
    finalize = function (reason = 'queue was garbage collected') {
      private$.state <- 'stopped'
      fmap(private$.workers, 'stop', reason)
      fmap(private$.jobs,    'stop', reason)
      return (invisible(NULL))
    },
    
    .uid         = NULL,
    .jobs        = list(),
    .workers     = list(),
    .loaded      = list(),
    .state       = 'starting',
    n_workers    = NULL,
    up_since     = NULL,
    total_runs   = 0L,
    job_defaults = list(),
    max_cpus     = NULL,
    
    poll_startup = function ()    q__poll_startup(self, private),
    dispatch     = function (...) q__dispatch(self, private)
  ),
  
  active = list(
    
    #' @field jobs
    #' List of [Job]s currently managed by this Queue.
    jobs = function () private$.jobs,
    
    #' @field workers
    #' List of [Worker]s used to process Jobs.
    workers = function () private$.workers,
    
    #' @field uid
    #' Unique identifier, e.g. 'Q1'.
    uid = function () private$.uid,
    
    #' @field loaded
    #' List of global variables and attached functions on the Workers.
    loaded = function () private$.loaded,
    
    #' @field state
    #' Current state: starting, active, stopped, or error.
    state = function () private$.state
  )
)


q_initialize <- function (
    self, private, 
    globals, packages, init, max_cpus, workers, options, 
    tmax, hooks, reformat, stop_id, copy_id ) {
  
  init_subst <- substitute(init, env = parent.frame())
  
  private$up_since  <- Sys.time()
  private$.uid      <- increment_uid('Q')
  
  private$globals   <- validate_list(globals)
  private$packages  <- validate_character_vector(packages)
  private$init      <- validate_expression(init, init_subst)
  private$max_cpus  <- validate_positive_integer(max_cpus, if_null = availableCores(omit = 1L))
  private$n_workers <- validate_positive_integer(workers,  if_null = ceiling(private$max_cpus * 1.2))
  private$options   <- validate_list(options,              if_null = r_session_options())
  
  private$job_defaults[['tmax']]     <- validate_tmax(tmax)
  private$job_defaults[['hooks']]    <- validate_hooks(hooks, 'JH')
  private$job_defaults[['reformat']] <- validate_function(reformat, bool_ok = TRUE, if_null = TRUE)
  private$job_defaults[['stop_id']]  <- validate_function(stop_id)
  private$job_defaults[['copy_id']]  <- validate_function(copy_id)
  
  private$poll_startup()
  
  return (invisible(self))
}


# Stop all jobs and prevent more from being added.
q_shutdown <- function (self, private, reason) {
  private$finalize(reason)
  private$.workers <- list()
  private$.jobs    <- list()
  return (invisible(self))
}


q_print <- function (self, private) {
  
  js     <- map(private$.jobs,    'state')
  ws     <- map(private$.workers, 'state')
  cpus   <- sum(map(private$.jobs[js == 'running'], 'cpus'))
  uptime <- format(round(Sys.time() - private$up_since))
  
  width <- min(50L, getOption("width"))
  rlang::local_options(cli.width = width)
  
  state <- switch(
    private$.state,
    starting = col_yellow('{.emph starting...}'),
    active   = col_green('active'),
    col_red('{private$.state}') )
  
  cli_rule(left = '{self$uid} {.cls {class(self)}}', right = state)
  cli_text("\n")
  
  div <- cli_div(theme = list('div.bullet-*' = list('margin-left' = 4)))
  cli_bullets(c('*' = '{.val {length(js)}} job{?s} - {.val {sum(js == "running")}} {?is/are} running'))
  cli_bullets(c('*' = '{.val {length(ws)}} worker{?s} - {.val {sum(ws == "busy")}} {?is/are} busy'))
  cli_bullets(c('*' = '{.val {cpus}} of {.val {private$max_cpus}} CPU{?s} {?is/are} currently in use'))
  
  for (i in setdiff(unique(ws), c('idle', 'busy')))
    cli_bullets(c('!' = '{.val {sum(ws == i)}} worker{?s} {?is/are} {col_red(i)}'))
  
  cli_end(div)
  
  cli_text("\n")
  cli_rule(center = col_grey('{private$total_runs} job{?s} run in {uptime}'))
  
  return (invisible(self))
}


q_run <- function (self, private, expr, vars, scan, ignore, tmax, hooks, reformat, cpus, stop_id, copy_id, start) {
  
  expr_subst <- substitute(expr, env = parent.frame())
  expr       <- validate_expression(expr, expr_subst, null_ok = FALSE)
  
  # Find globals in expr and add them to vars.
  if (!is_false(scan)) {
    
    if (is_true(scan)) { envir <- parent.frame(n = 2)        }
    else               { envir <- validate_environment(scan) }
    
    vars   <- validate_list(vars, if_null = list())
    ignore <- validate_character_vector(ignore)
  
    add <- globalsOf(expr, envir = envir, method = "liberal", mustExist = FALSE)
    pkg <- sapply(attr(add, 'where'), env_name)
    nms <- setdiff(names(add), c(names(vars), ignore, private$.loaded$globals))
    
    for (nm in nms)
      if (!identical(pkg[[nm]], private$.loaded$attached[[nm]]))
        vars[[nm]] <- add[[nm]]
  }
  
  # Replace NA values with defaults from Queue$new() call.
  if (is_na(tmax))     tmax     <- private$job_defaults[['tmax']]
  if (is_na(hooks))    hooks    <- private$job_defaults[['hooks']]
  if (is_na(reformat)) reformat <- private$job_defaults[['reformat']]
  
  job <- Job$new(
    expr     = expr, 
    vars     = vars, 
    tmax     = tmax,
    hooks    = hooks,
    reformat = reformat,
    cpus     = cpus )
  
  # Compute stop_id and copy_id, but don't act on them yet.
  for (id in c('stop_id', 'copy_id')) {
    x <- get(id, inherits = FALSE)
    if      (is_na(x))      { x <- private$job_defaults[[id]]  }
    else if (is_formula(x)) { x <- as_function(x)              }
    job[[id]] <- if (is_function(x)) x(job) else x
  }
  
  # Option to not start the job just yet.
  if (is_true(start))
    self$submit(job)
  
  return (invisible(job))
}


q_submit <- function (self, private, job) {
  
  if (!inherits(job, 'Job'))
    cli_abort('`job` must be a Job object, not {.type {job}}.')
  
  if (!(private$.state %in% c('starting', 'active')))
    cli_abort('Queue cannot accept new jobs. State is "{col_red(private$.state)}".')
  
  job$queue          <- self
  job$state          <- 'submitted'
  private$total_runs <- private$total_runs + 1L
  
  # Check for `stop_id` hash collision => stop the other job.
  if (!is_null(job$stop_id))
    if (nz(stop_jobs <- get_eq(private$.jobs, 'stop_id', job$stop_id)))
      fmap(stop_jobs, 'stop', 'duplicated stop_id')
  
  # Check for `copy_id` hash collision => proxy the other job.
  if (!is_null(job$copy_id))
    if (nz(copy_jobs <- get_eq(private$.jobs, 'copy_id', job$copy_id)))
      job$proxy <- copy_jobs[[1]]
  
  if (job$state == 'submitted') {
    private$.jobs %<>% c(job)
    job$state <- 'queued'
    private$dispatch()
  }
  
  return (invisible(job))
}



# Use any idle workers to run queued jobs.
q__dispatch <- function (self, private) {
  
  # Purge jobs that are done.
  private$.jobs <- get_eq(private$.jobs, 'is_done', FALSE)
  
  # Only start jobs if this Queue is active.
  if (private$.state != 'active')
    return (invisible(NULL))
  
  # See how many remaining CPUs are available.
  running   <- get_eq(private$.workers, 'state', 'running')
  free_cpus <- private$max_cpus - sum(map(running, 'cpus'))
  if (free_cpus < 1) return (invisible(NULL))
  
  # Connect queued jobs to idle workers.
  jobs    <- get_eq(private$.jobs,    'state', 'queued')
  workers <- get_eq(private$.workers, 'state', 'idle')
  for (i in seq_len(min(length(workers), length(jobs)))) {
    free_cpus <- free_cpus - jobs[[i]]$cpus
    if (free_cpus < 0) break
    workers[[i]]$run(jobs[[i]])
  }
  
  return (invisible(NULL))
}


# Creates `n_workers`, at most `max_cpus` starting at a time.
q__poll_startup <- function (self, private) {
  
  states <- map(private$.workers, 'state')
  
  if (any(states == 'stopped')) {
    
    self$shutdown('worker process did not start cleanly')
    private$.state <- 'error'
  }
  
  else if (sum(states == 'idle') == private$n_workers) {
    
    loaded <- private$.workers[[1]]$loaded
    private$.loaded[['globals']]  <- loaded[['globals']]
    private$.loaded[['packages']] <- loaded[['packages']]
    
    private$.state <- 'active'
    private$dispatch()
  }
  
  else {
    
    n <- min(
      private$n_workers - length(states), 
      private$max_cpus - sum(states != 'idle') )
    
    for (i in integer(n)) {
      
      worker <- Worker$new(
        globals  = private$globals, 
        packages = private$packages, 
        init     = private$init, 
        options  = private$options,
        hooks    = list('idle' = private$dispatch) )
      
      private$.workers %<>% c(worker)
    }
    
    later(private$poll_startup, 0.2)
  }
  
}
