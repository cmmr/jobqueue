
#' Assigns Jobs to a Set of Workers
#'
#' @name Queue
#'
#' @description
#' 
#' Jobs go in. Results come out.
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
#'        `<Job>$result` should return. The default, `reformat = NULL` returns 
#'        `<Job>$output` as `<Job>$result`. See `vignette('results')`.
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
#' @param max_cpus  Total number of CPU cores that can be reserved by all 
#'        running Jobs (`sum(cpus)`). Does not enforce limits on actual CPU 
#'        utilization.
#'        
#' @param workers  How many background [Worker] processes to start. Set to more 
#'        than `max_cpus` to enable interrupted workers to be quickly swapped 
#'        out with standby Workers while a replacement Worker boots up.
#'        
#' @param job  A [Job] object, as created by `Job$new()`.
#'        
#' @param stop_id  If an existing [Job] in the Queue has the same `stop_id`,  
#'        that Job will be stopped and return an 'interrupt' condition object 
#'        as its result. `stop_id` can also be a `function (job)` that returns 
#'        the `stop_id` to assign to a given Job. A `stop_id` of `NULL` 
#'        disables this feature. See `vignette('stops')`.
#'                 
#' @param copy_id  If an existing [Job] in the Queue has the same `copy_id`, 
#'        the newly submitted Job will become a "proxy" for that earlier Job, 
#'        returning whatever result the earlier Job returns. `copy_id` can also 
#'        be a `function (job)` that returns the `copy_id` to assign to a given 
#'        Job. A `copy_id` of `NULL` disables this feature. 
#'        See `vignette('stops')`.
#'        
#' @param reason,cls  Passed to `<Job>$stop(reason, cls)` for any Jobs 
#'        currently managed by this Queue.
#'        
#' @param state  The Queue state that will trigger this function. One of:
#'        \describe{
#'            \item{`'*'` -        }{ Every time the state changes. }
#'            \item{`'.next'` -    }{ Only one time, the next time the state changes. }
#'            \item{`'starting'` - }{ Workers are starting. }
#'            \item{`'idle'` -     }{ All workers are ready/idle. }
#'            \item{`'busy'` -     }{ At least one worker is busy. }
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
    #' `globals`, `packages`, and `init` arguments. The Queue will 
    #' not use more than `max_cpus` at once, assuming the `cpus` argument is 
    #' properly set for each Job.
    #'
    #' @param timeout,hooks,reformat,signal,cpus,stop_id,copy_id
    #'        Defaults for this Queue's `$run()` method. Here only, `stop_id` and 
    #'        `copy_id` must be either a `function (job)` or `NULL`, and `hooks`
    #'        can take on an alternate format as described in the Callback Hooks
    #'        section.
    #'
    #' @return A `Queue` object.
    initialize = function (
        globals   = NULL,
        packages  = NULL,
        init      = NULL,
        max_cpus  = detectCores(),
        workers   = ceiling(max_cpus * 1.2),
        timeout   = NULL,
        hooks     = NULL,
        reformat  = NULL,
        signal    = FALSE,
        cpus      = 1L,
        stop_id   = NULL,
        copy_id   = NULL ) {
       
      q_initialize(
        self, private, 
        globals, packages, init, max_cpus, workers, 
        timeout, hooks, reformat, signal, cpus, 
        stop_id, copy_id )
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
        timeout  = NA, 
        hooks    = NA, 
        reformat = NA, 
        signal   = NA, 
        cpus     = NA, 
        stop_id  = NA, 
        copy_id  = NA, 
        ... ) {
      
      q_run(
        self, private, 
        expr, vars, 
        timeout, hooks, reformat, signal, 
        cpus, stop_id, copy_id, ... )
    },
    
    
    #' @description
    #' Adds a Job to the Queue for running on a background process.
    #' @return This Queue, invisibly.
    submit = function (job)
      q_submit(self, private, job),
    
    #' @description
    #' Blocks until the Queue enters the given state.
    #' @return This Queue, invisibly.
    wait = function (state = 'idle') u_wait(self, private, state),
    
    #' @description
    #' Attach a callback function.
    #' @return A function that when called removes this callback from the Queue.
    on = function (state, func) u_on(self, private, 'QH', state, func),
    
    #' @description
    #' Stop all jobs and workers.
    #' @return This Queue, invisibly.
    stop = function (reason = 'job queue shut down by user', cls = NULL) {
      q_stop(self, private, reason, cls)
    }
  ),


  private = list(
    
    finalize = function (reason = 'queue was garbage collected', cls = NULL) {
      private$is_ready <- FALSE
      fmap(self$workers, 'stop', reason, cls)
      fmap(self$jobs,    'stop', reason, cls)
      return (invisible(NULL))
    },
    
    .uid         = NULL,
    .hooks       = list(),
    .jobs        = list(),
    .workers     = list(),
    .state       = 'initializing',
    
    n_workers    = NULL,
    up_since     = NULL,
    total_runs   = 0L,
    is_ready     = FALSE,
    j_conf       = list(),
    w_conf       = list(),
    max_cpus     = NULL,
    
    set_state    = function (state) u__set_state(self, private, state),
    poll_startup = function ()      q__poll_startup(self, private),
    dispatch     = function (...)   q__dispatch(self, private)
  ),
  
  active = list(
    
    #' @field uid
    #' Get or set - Unique identifier, e.g. `'Q1'`.
    uid = function (value) q_uid(private, value),
    
    #' @field jobs
    #' Get or set - List of [Job]s currently managed by this Queue.
    jobs = function (value) q_jobs(private, value),
    
    #' @field workers
    #' Get or set - List of [Worker]s used for processing Jobs.
    workers = function (value) q_workers(private, value),
    
    #' @field hooks
    #' A named list of currently registered callback hooks.
    hooks = function () private$.hooks,
    
    #' @field state
    #' Current state: `'starting'`, `'idle'`, `'busy'`, `'stopped'`, or `'error.'`
    state = function () private$.state
  )
)


q_initialize <- function (
    self, private, 
    globals, packages, init, max_cpus, workers, 
    timeout, hooks, reformat, signal, cpus, 
    stop_id, copy_id ) {
  
  init_subst       <- substitute(init, env = parent.frame())
  private$up_since <- Sys.time()
  
  # Assign hooks by q_ and w_ prefixes
  hooks <- validate_list(hooks)
  if (!all(names(hooks) %in% c('queue', 'worker', 'job')))
    hooks <- list(
      job    = hooks[grep('^[qwj]_', names(hooks), invert = TRUE)], 
      queue  = hooks[grep('^q_',     names(hooks))], 
      worker = hooks[grep('^w_',     names(hooks))] )
  hooks[['worker']] <- c(list('idle' = private$dispatch), hooks[['worker']])
  
  # Queue configuration
  self$uid          <- increment_uid('Q')
  private$.hooks    <- validate_hooks(hooks[['queue']], 'QH')
  private$max_cpus  <- validate_positive_integer(max_cpus, if_null = detectCores())
  private$n_workers <- validate_positive_integer(workers,  if_null = ceiling(private$max_cpus * 1.2))
  
  # Worker configuration
  private$w_conf[['globals']]  <- validate_list(globals, if_null = NULL)
  private$w_conf[['packages']] <- validate_character_vector(packages)
  private$w_conf[['init']]     <- validate_expression(init, init_subst)
  private$w_conf[['hooks']]    <- validate_hooks(hooks[['worker']], 'WH')
  
  # Job configuration defaults
  private$j_conf[['timeout']]  <- validate_timeout(timeout)
  private$j_conf[['hooks']]    <- validate_hooks(hooks[['job']], 'JH')
  private$j_conf[['reformat']] <- validate_function(reformat)
  private$j_conf[['signal']]   <- validate_character_vector(signal, bool_ok = TRUE)
  private$j_conf[['cpus']]     <- validate_positive_integer(cpus, if_null = 1L)
  private$j_conf[['stop_id']]  <- validate_function(stop_id)
  private$j_conf[['copy_id']]  <- validate_function(copy_id)
  
  private$set_state('starting')
  later(private$poll_startup)
  
  return (invisible(self))
}


q_print <- function (self, private) {
  
  js     <- map(self$jobs,    'state')
  ws     <- map(self$workers, 'state')
  cpus   <- sum(map(self$jobs[js == 'running'], 'cpus'))
  uptime <- format(round(Sys.time() - private$up_since))
  
  width <- min(50L, getOption("width"))
  rlang::local_options(cli.width = width)
  
  state <- switch(
    private$.state,
    starting = col_yellow('{.emph starting...}'),
    idle     = col_green('idle'),
    busy     = col_green('busy'),
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
    expr, vars, 
    timeout, hooks, reformat, signal, 
    cpus, stop_id, copy_id, ...) {
  
  expr_subst <- substitute(expr, env = parent.frame())
  expr       <- validate_expression(expr, expr_subst, null_ok = FALSE)
  
  # Replace NA values with defaults from Queue$new() call.
  j_conf <- private$j_conf
  
  if (is_formula(stop_id)) stop_id <- as_function(stop_id)
  if (is_formula(copy_id)) copy_id <- as_function(copy_id)
  
  job <- Job$new(
    expr     = expr, 
    vars     = vars,
    queue    = self,
    timeout  = if (is_na(timeout))  j_conf[['timeout']]  else timeout,
    hooks    = if (is_na(hooks))    j_conf[['hooks']]    else hooks,
    reformat = if (is_na(reformat)) j_conf[['reformat']] else reformat,
    signal   = if (is_na(signal))   j_conf[['signal']]   else signal,
    cpus     = if (is_na(cpus))     j_conf[['cpus']]     else cpus,
    stop_id  = if (is_na(stop_id))  j_conf[['stop_id']]  else stop_id,
    copy_id  = if (is_na(copy_id))  j_conf[['copy_id']]  else copy_id,
    ... )
  
  job$caller_env <- caller_env(2L)
  self$submit(job)
  
  return (invisible(job))
}


q_submit <- function (self, private, job) {
  
  if (!inherits(job, 'Job'))
    cli_abort('`job` must be a Job object, not {.type {job}}.')
  
  if (job$is_done) return (invisible(job))
  
  if (is_null(job$caller_env))
    job$caller_env <- caller_env(2L)
  
  job$queue          <- self
  private$total_runs <- private$total_runs + 1L
  
  job$state <- 'submitted'
  if (job$state != 'submitted')
    return (invisible(job))
  
  # Check for `stop_id` hash collision => stop the other job.
  if (is_formula(job$stop_id))  job$stop_id <- as_function(job$stop_id)
  if (is_function(job$stop_id)) job$stop_id <- job$stop_id(job)
  if (!is_null(id <- job$stop_id))
    if (nz(stop_jobs <- get_eq(self$jobs, 'stop_id', id)))
      fmap(stop_jobs, 'stop', 'duplicated stop_id', 'superseded')
  
  # Check for `copy_id` hash collision => proxy the other job.
  if (is_formula(job$copy_id))  job$copy_id <- as_function(job$copy_id)
  if (is_function(job$copy_id)) job$copy_id <- job$copy_id(job)
  if (!is_null(id <- job$copy_id))
    if (nz(copy_jobs <- get_eq(self$jobs, 'copy_id', id)))
      job$proxy <- copy_jobs[[1]]
  
  if (job$state == 'submitted') {
    self$jobs <- c(self$jobs, list(job))
    job$state <- 'queued'
    private$dispatch()
  }
  
  return (invisible(job))
}


# Stop all jobs and prevent more from being added.
q_stop <- function (self, private, reason, cls) {
  private$finalize(reason, cls)
  private$set_state('stopped')
  self$workers <- list()
  self$jobs    <- list()
  return (invisible(self))
}


# Use any idle workers to run queued jobs.
q__dispatch <- function (self, private) {
  
  # Purge jobs that are done.
  self$jobs <- get_eq(self$jobs, 'is_done', FALSE)
  
  # Only start jobs if this Queue is ready.
  if (!private$is_ready) return (invisible(NULL))
  
  # See how many remaining CPUs are available.
  running_jobs <- get_eq(self$jobs, 'state', 'running')
  free_cpus    <- private$max_cpus - sum(map(running_jobs, 'cpus'))
  if (free_cpus < 1) return (invisible(NULL))
  
  # Connect queued jobs to idle workers.
  idle_workers <- get_eq(self$workers, 'state', 'idle')
  queued_jobs  <- get_eq(self$jobs,    'state', 'queued')
  queued_jobs  <- queued_jobs[cumsum(map(queued_jobs, 'cpus')) <= free_cpus]
  for (i in seq_len(min(length(idle_workers), length(queued_jobs)))) {
    try(idle_workers[[i]]$run(queued_jobs[[i]]))
  }
  
  # Update <Queue>$state and trigger callback hooks.
  busy_workers <- get_eq(self$workers, 'state', 'busy')
  queue_state  <- ifelse(length(busy_workers) > 0, 'busy', 'idle')
  private$set_state(state = queue_state)
  
  return (invisible(NULL))
}


# Creates `n_workers`, at most `max_cpus` starting at a time.
q__poll_startup <- function (self, private) {
  
  states <- map(self$workers, 'state')
  
  if (any(states == 'stopped')) {
    
    self$stop('worker process did not start cleanly')
    private$set_state('error')
  }
  
  else if (sum(states == 'idle') == private$n_workers) {
    
    private$is_ready <- TRUE
    private$set_state('idle')
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
        hooks    = private$w_conf[['hooks']] )
      
      self$workers %<>% c(worker)
    }
    
    later(private$poll_startup, delay = 0.2)
  }
  
}


# Active bindings to validate new values.

q_uid <- function (private, value) {
  if (missing(value)) return (private$.uid)
  private$.uid <- validate_string(value)
}

q_jobs <- function (private, value) {
  if (missing(value)) return (private$.jobs)
  private$.jobs <- validate_list(value, of_type = 'Job', named = FALSE)
}

q_workers <- function (private, value) {
  if (missing(value)) return (private$.workers)
  private$.workers <- validate_list(value, of_type = 'Worker', named = FALSE)
}
