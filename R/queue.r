
#' Initialize and Run Jobs on a Set of Workers.
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
#' The [Job] object also has a `$stop()` method which can be called to return 
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
#' @param scan  Should additional variables be added to `vars` based on 
#'        scanning `expr` for missing global variables? The default, 
#'        `scan = TRUE` always scans and adds to `vars`, set `scan = FALSE` to 
#'        never scan, and `scan = <an environment-like object>` to look for 
#'        globals there. `vars` defined by the user are always left untouched.
#' 
#' @param ignore  A character vector of variable names that should NOT be added 
#'        to `vars` by `scan`.
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
#' @param lazy  When `lazy = FALSE` (the default), the job is submitted for 
#'        evaluation immediately. Otherwise it is lazily evaluated - that is, 
#'        only once `<Job>$result`, `<Job>$output`, or `<Queue>$submit(<Job>)` 
#'        is called.
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
#' @param reason  Passed to `<Job>$stop(reason)` for any Jobs currently 
#'        managed by this Queue.
#'        
#' @param state  The Queue state that will trigger this function. One of:
#'        \describe{
#'            \item{`'*'` -        }{ Every time the state changes. }
#'            \item{`'.next'` -    }{ Only one time, the next time the state changes. }
#'            \item{`'starting'` - }{ Workers are starting. }
#'            \item{`'active'` -   }{ Workers are ready. }
#'            \item{`'stopped'` -  }{ Shutdown is complete. }
#'            \item{`'error'` -    }{ Workers did not start cleanly. }
#'        }
#'        
#' @param func  A function that accepts a Queue object as input. Return value 
#'        is ignored.
#'        
#' @param ...  Arbitrary named values to add to the returned Job object.
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
    #' @param scan,ignore,envir,timeout,hooks,reformat,cpus,stop_id,copy_id,lazy
    #'        Defaults for this Queue's `$run()` method. Here only, 
    #'        
    #'          * `stop_id` and `copy_id` must be a `function (job)` or `NULL`.
    #'          * `hooks` can be a list of lists with the form:  
    #'            `hooks = list(queue = list(), worker = list(), job = list())`.
    #'          * `hooks` can be prefixed with `q_` or `w_` to assign them to
    #'            the queue or workers, respectively, instead of to jobs, e.g.
    #'            `hooks = list(q_active = function (queue) {...}, * = ~{...})`
    #'
    #' @return A `Queue` object.
    initialize = function (
        globals   = NULL,
        packages  = NULL,
        init      = NULL,
        max_cpus  = availableCores(omit = 1L),
        workers   = ceiling(max_cpus * 1.2),
        options   = r_session_options(),
        scan      = TRUE,
        ignore    = NULL,
        envir     = NULL,
        timeout      = NULL,
        hooks     = NULL,
        reformat  = TRUE,
        cpus      = 1L,
        stop_id   = NULL,
        copy_id   = NULL,
        lazy      = FALSE ) {
       
      q_initialize(
        self, private, 
        globals, packages, init, max_cpus, workers, options,
        scan, ignore, envir, timeout, hooks, reformat, cpus, 
        stop_id, copy_id, lazy )
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
        vars     = list(), 
        scan     = NA, 
        ignore   = NA,
        envir    = NA,
        timeout  = NA, 
        hooks    = NA, 
        reformat = NA, 
        cpus     = NA, 
        stop_id  = NA, 
        copy_id  = NA, 
        lazy     = NA,
        ... ) {
      
      q_run(
        self, private, 
        expr, vars, 
        scan, ignore, envir, timeout, hooks, reformat, cpus, 
        stop_id, copy_id, lazy, ... )
    },
    
    
    #' @description
    #' Adds a Job to the Queue for running on a background process.
    #' @return This Queue, invisibly.
    submit = function (job)
      q_submit(self, private, job),
    
    #' @description
    #' Blocks until the Queue enters the given state.
    #' @return This Queue, invisibly.
    wait = function (state = 'active') u_wait(self, private, state),
    
    #' @description
    #' Attach a callback function.
    #' @return A function that when called removes this callback from the Queue.
    on = function (state, func) u_on(self, private, 'QH', state, func),
    
    #' @description
    #' Stop all jobs and workers.
    #' @return This Queue, invisibly.
    shutdown = function (reason = 'job queue shut down by user') {
      q_shutdown(self, private, reason)
    }
  ),


  private = list(
    
    finalize = function (reason = 'queue was garbage collected') {
      private$set_state('stopped')
      fmap(private$.workers, 'stop', reason)
      fmap(private$.jobs,    'stop', reason)
      return (invisible(NULL))
    },
    
    .hooks       = list(),
    .uid         = NULL,
    .jobs        = list(),
    .workers     = list(),
    .loaded      = list(),
    .state       = 'starting',
    n_workers    = NULL,
    up_since     = NULL,
    total_runs   = 0L,
    j_conf       = list(),
    w_conf       = list(),
    max_cpus     = NULL,
    
    set_state    = function (state) u__set_state(self, private, state),
    poll_startup = function ()      q__poll_startup(self, private),
    dispatch     = function (...)   q__dispatch(self, private)
  ),
  
  active = list(
    
    #' @field hooks
    #' A named list of currently registered callback hooks.
    hooks = function () private$.hooks,
    
    #' @field jobs
    #' List of [Job]s currently managed by this Queue.
    jobs = function () private$.jobs,
    
    #' @field workers
    #' List of [Worker]s used for processing Jobs.
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
    scan, ignore, envir, timeout, hooks, reformat, cpus, 
    stop_id, copy_id, lazy ) {
  
  init_subst <- substitute(init, env = parent.frame())
  
  # Assign hooks by q_ and w_ prefixes
  hooks <- validate_list(hooks, if_null = list())
  if (!all(names(hooks) %in% c('queue', 'worker', 'job')))
    hooks <- list(
      job    = hooks[grep('^[qw]_', names(hooks), invert = TRUE)], 
      queue  = hooks[grep('^q_',    names(hooks))], 
      worker = hooks[grep('^w_',    names(hooks))] )
  hooks[['worker']] %<>% c(list('idle' = private$dispatch), .)
  
  # Queue configuration
  private$up_since  <- Sys.time()
  private$.uid      <- increment_uid('Q')
  private$.hooks    <- validate_hooks(hooks[['queue']],  'QH')
  private$max_cpus  <- validate_positive_integer(max_cpus, if_null = availableCores(omit = 1L))
  private$n_workers <- validate_positive_integer(workers,  if_null = ceiling(private$max_cpus * 1.2))
  private$options   <- validate_list(options,              if_null = r_session_options())
  
  # Worker configuration
  private$w_conf[['globals']]  <- validate_list(globals)
  private$w_conf[['packages']] <- validate_character_vector(packages)
  private$w_conf[['init']]     <- validate_expression(init, init_subst)
  private$w_conf[['hooks']]    <- validate_hooks(hooks[['worker']], 'WH')
  
  # Job configuration defaults
  private$j_conf[['scan']]     <- validate_logical(scan)
  private$j_conf[['ignore']]   <- validate_character_vector(ignore)
  private$j_conf[['envir']]    <- validate_environment(envir, null_ok = TRUE)
  private$j_conf[['timeout']]  <- validate_timeout(timeout)
  private$j_conf[['hooks']]    <- validate_hooks(hooks[['job']], 'JH')
  private$j_conf[['reformat']] <- validate_function(reformat, bool_ok = TRUE, if_null = TRUE)
  private$j_conf[['cpus']]     <- validate_positive_integer(cpus, if_null = 1L)
  private$j_conf[['stop_id']]  <- validate_function(stop_id)
  private$j_conf[['copy_id']]  <- validate_function(copy_id)
  private$j_conf[['lazy']]     <- validate_logical(lazy)
  
  private$poll_startup()
  
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


q_run <- function (
    self, private, 
    expr, vars, scan, ignore, envir, 
    timeout, hooks, reformat, cpus, stop_id, copy_id, lazy, ...) {
  
  expr_subst <- substitute(expr, env = parent.frame())
  expr       <- validate_expression(expr, expr_subst, null_ok = FALSE)
  
  # Replace NA values with defaults from Queue$new() call.
  j_conf <- private$j_conf
  
  if (is_null(j_conf[['envir']]))
    j_conf[['envir']] <- parent.frame(n = 2)
  
  if (is_formula(stop_id)) stop_id <- as_function(stop_id)
  if (is_formula(copy_id)) copy_id <- as_function(copy_id)
  
  job <- Job$new(
    expr     = expr, 
    vars     = vars,
    queue    = self,
    scan     = if (is_na(scan))     j_conf[['scan']]     else scan,
    ignore   = if (is_na(ignore))   j_conf[['ignore']]   else ignore,
    envir    = if (is_na(envir))    j_conf[['envir']]    else envir,
    timeout  = if (is_na(timeout))  j_conf[['timeout']]  else timeout,
    hooks    = if (is_na(hooks))    j_conf[['hooks']]    else hooks,
    reformat = if (is_na(reformat)) j_conf[['reformat']] else reformat,
    cpus     = if (is_na(cpus))     j_conf[['cpus']]     else cpus,
    stop_id  = if (is_na(stop_id))  j_conf[['stop_id']]  else stop_id,
    copy_id  = if (is_na(copy_id))  j_conf[['copy_id']]  else copy_id,
    ... )
  
  # Option to not start the job just yet.
  if (is_na(lazy))    lazy <- j_conf[['lazy']]
  if (is_false(lazy)) self$submit(job)
  
  return (invisible(job))
}


q_submit <- function (self, private, job) {
  
  if (!inherits(job, 'Job'))
    cli_abort('`job` must be a Job object, not {.type {job}}.')
  
  job$queue          <- self
  private$total_runs <- private$total_runs + 1L
  
  job$state <- 'submitted'
  if (job$state != 'submitted')
    return (invisible(job))
  
  # Find globals in expr and add them to vars.
  if (is_true(job$scan)) {
    
    expr     <- job$expr
    vars     <- job$vars %||% list()
    envir    <- job$envir
    ignore   <- job$ignore
    
    if (private$.state != 'active') self$wait()
    globals  <- private$.loaded$globals
    attached <- private$.loaded$attached
    
    add <- globalsOf(expr, envir, method = "liberal", mustExist = FALSE)
    add <- cleanup(add)
    pkg <- sapply(attr(add, 'where'), env_name)
    nms <- setdiff(names(add), c(names(vars), ignore, globals))
    
    for (nm in nms)
      if (!identical(pkg[[nm]], attached[[nm]]))
        vars[[nm]] <- add[[nm]]
    
    job$vars <- vars
  }
  
  # Compute stop_id and copy_id.
  if (is_function(job$stop_id)) job$stop_id <- job$stop_id(job)
  if (is_function(job$copy_id)) job$copy_id <- job$copy_id(job)
  
  # Check for `stop_id` hash collision => stop the other job.
  if (!is_null(id <- job$stop_id))
    if (nz(stop_jobs <- get_eq(private$.jobs, 'stop_id', id)))
      fmap(stop_jobs, 'stop', 'duplicated stop_id')
  
  # Check for `copy_id` hash collision => proxy the other job.
  if (!is_null(id <- job$copy_id))
    if (nz(copy_jobs <- get_eq(private$.jobs, 'copy_id', id)))
      job$proxy <- copy_jobs[[1]]
  
  if (job$state == 'submitted') {
    private$.jobs %<>% c(job)
    job$state <- 'queued'
    private$dispatch()
  }
  
  return (invisible(job))
}


# Stop all jobs and prevent more from being added.
q_shutdown <- function (self, private, reason) {
  private$finalize(reason)
  private$.workers <- list()
  private$.jobs    <- list()
  return (invisible(self))
}


# Use any idle workers to run queued jobs.
q__dispatch <- function (self, private) {
  
  # Purge jobs that are done.
  private$.jobs <- get_eq(private$.jobs, 'is_done', FALSE)
  
  # Only start jobs if this Queue is active.
  if (private$.state != 'active')
    return (invisible(NULL))
  
  # See how many remaining CPUs are available.
  running_jobs <- get_eq(private$.jobs, 'state', 'running')
  free_cpus    <- private$max_cpus - sum(map(running_jobs, 'cpus'))
  if (free_cpus < 1) return (invisible(NULL))
  
  # Connect queued jobs to idle workers.
  workers     <- get_eq(private$.workers, 'state', 'idle')
  queued_jobs <- get_eq(private$.jobs,    'state', 'queued')
  queued_jobs <- queued_jobs[cumsum(map(queued_jobs, 'cpus')) <= free_cpus]
  for (i in seq_len(min(length(workers), length(queued_jobs)))) {
    workers[[i]]$run(queued_jobs[[i]])
  }
  
  return (invisible(NULL))
}


# Creates `n_workers`, at most `max_cpus` starting at a time.
q__poll_startup <- function (self, private) {
  
  states <- map(private$.workers, 'state')
  
  if (any(states == 'stopped')) {
    
    self$shutdown('worker process did not start cleanly')
    private$set_state('error')
  }
  
  else if (sum(states == 'idle') == private$n_workers) {
    
    private$.loaded <- private$.workers[[1]]$loaded
    
    private$set_state('active')
    private$dispatch()
  }
  
  else {
    
    n <- min(
      private$n_workers - length(states), 
      private$max_cpus - sum(states != 'idle') )
    
    for (i in integer(n)) {
      
      worker <- Worker$new(
        globals  = private$w_conf[['globals']], 
        packages = private$w_conf[['packages']], 
        init     = private$w_conf[['init']], 
        options  = private$w_conf[['options']],
        hooks    = private$w_conf[['hooks']] )
      
      private$.workers %<>% c(worker)
    }
    
    later(private$poll_startup, 0.2)
  }
  
}
