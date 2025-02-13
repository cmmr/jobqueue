
#' Assigns Jobs to a Set of Workers
#'
#' @name Queue
#'
#' @description
#' 
#' Jobs go in. Results come out.
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
#' @param expr  A call or R expression wrapped in curly braces to evaluate on a 
#'        worker. Will have access to any variables defined by `vars`, as well 
#'        as the Worker's `globals`, `packages`, and `init` configuration.
#'        See `vignette('eval')`.
#' 
#' @param vars  A named list of variables to make available to `expr` during 
#'        evaluation. Alternatively, an object that can be coerced to a named 
#'        list with `as.list()`, e.g. named vector, data.frame, or environment. 
#'        Or a `function (job)` that returns such an object.
#' 
#' @param timeout  A named numeric vector indicating the maximum number of 
#'        seconds allowed for each state the job passes through, or 'total' to
#'        apply a single timeout from 'submitted' to 'done'. Or a 
#'        `function (job)` that returns the same. Example:
#'        `timeout = c(total = 2.5, running = 1)`. See `vignette('stops')`.
#'        
#' @param hooks  A named list of functions to run when the Job state changes, 
#'        of the form `hooks = list(created = function (worker) {...})`. Or a 
#'        `function (job)` that returns the same. Names of worker hooks are 
#'        typically `'created'`, `'submitted'`, `'queued'`, `'dispatched'`, 
#'        `'starting'`, `'running'`, `'done'`, or `'*'` (duplicates okay). 
#'        See `vignette('hooks')`.
#'        
#' @param reformat  Set `reformat = function (job)` to define what 
#'        `<Job>$result` should return. The default, `reformat = NULL` passes 
#'        `<Job>$output` to `<Job>$result` unchanged.
#'        See `vignette('results')`.
#'        
#' @param signal  Should calling `<Job>$result` signal on condition objects?
#'        When `FALSE`, `<Job>$result` will return the object without 
#'        taking additional action. Setting to `TRUE` or a character vector of 
#'        condition classes, e.g. `c('interrupt', 'error', 'warning')`, will
#'        cause the equivalent of `stop(<condition>)` to be called when those
#'        conditions are produced. Alternatively, a `function (job)` that 
#'        returns `TRUE` or `FALSE`. See `vignette('results')`.
#'        
#' @param cpus  How many CPU cores to reserve for this Job.  Or a 
#'        `function (job)` that returns the same. Used to limit the number of 
#'        Jobs running simultaneously to respect `<Queue>$max_cpus`. Does not 
#'        prevent a Job from using more CPUs than reserved.
#' 
#' @param max_cpus  Total number of CPU cores that can be reserved by all 
#'        running Jobs (`sum(<Job>$cpus)`). Does not enforce limits on actual 
#'        CPU utilization.
#'        
#' @param workers  How many background [Worker] processes to start. Set to more 
#'        than `max_cpus` to enable standby Workers to quickly swap out with 
#'        Workers that need to restart.
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
#' @param wait  If `TRUE`, blocks until the Queue is 'idle'. If `FALSE`, the 
#'        Queue object is returned in the 'starting' state. If a number, blocks 
#'        at most that number of seconds before returning either an 'idle' or 
#'        'stopped' Queue.
#'        
#' @param reason  Passed to `<Job>$stop()` for any Jobs currently managed by 
#'         this Queue.
#'        
#' @param cls  Passed to `<Job>$stop()` for any Jobs currently managed by 
#'         this Queue.
#'        
#' @param state
#' The name of a Queue state. Typically one of:
#' 
#' * `'*'` -        Every time the state changes.
#' * `'.next'` -    Only one time, the next time the state changes.
#' * `'starting'` - Workers are starting.
#' * `'idle'` -     All workers are ready/idle.
#' * `'busy'` -     At least one worker is busy.
#' * `'stopped'` -  Shutdown is complete.
#' * `'error'` -    Workers did not start cleanly.
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
    #' Creates a pool of background processes for handling `$run()` and 
    #' `$submit()` calls. These workers are initialized according to the 
    #' `globals`, `packages`, and `init` arguments.
    #'
    #' @param timeout,hooks,reformat,signal,cpus,stop_id,copy_id
    #'        Defaults for this Queue's `$run()` method. Here only, `stop_id` 
    #'        and `copy_id` must be either a `function (job)` or `NULL`. 
    #'        `hooks` can set queue, worker, and/or job hooks - see the 
    #'        "Attaching" section in `vignette('hooks')`.
    #'
    #' @return A `Queue` object.
    initialize = function (
        globals   = NULL,
        packages  = NULL,
        init      = NULL,
        max_cpus  = availableCores(),
        workers   = ceiling(max_cpus * 1.2),
        timeout   = NULL,
        hooks     = NULL,
        reformat  = NULL,
        signal    = FALSE,
        cpus      = 1L,
        stop_id   = NULL,
        copy_id   = NULL,
        wait      = TRUE ) {
       
      q_initialize(
        self, private, 
        globals, packages, init, max_cpus, workers, 
        timeout, hooks, reformat, signal, cpus, 
        stop_id, copy_id, wait )
    },
    
    
    #' @description
    #' Print method for a `Queue`.
    #' @param ... Arguments are not used currently.
    print = function (...) q_print(self, private),
    
    
    #' @description
    #' Creates a Job object and submits it to the queue for running. 
    #' Any `NA` arguments will be replaced with their value from `Queue$new()`.
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
    #' @param timeout Stop the Queue if it takes longer than this number of seconds, or `NULL`.
    #' @return This Queue, invisibly.
    wait = function (state = 'idle', timeout = NULL) 
      u_wait(self, private, state, timeout),
    
    #' @description
    #' Attach a callback function to execute when the Queue enters `state`.
    #' @return A function that when called removes this callback from the Queue.
    on = function (state, func) 
      u_on(self, private, 'QH', state, func),
    
    #' @description
    #' Stop all jobs and workers.
    #' @return This Queue, invisibly.
    stop = function (reason = 'job queue shut down by user', cls = NULL) {
      q_stop(self, private, reason, cls)
    }
  ),


  private = list(
    
    finalize = function (reason = 'queue was garbage collected', cls = NULL) {
      
      fmap(private$.workers, 'stop', reason, cls)
      fmap(private$.jobs,    'stop', reason, cls)
      
      unlink(private$.tmp, recursive = TRUE, expand = FALSE)
      
      return (invisible(NULL))
    },
    
    .uid         = NULL,
    .tmp         = NULL,
    .hooks       = list(),
    .jobs        = list(),
    .workers     = list(),
    .state       = 'initializing',
    .is_done     = FALSE,
    
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
    
    #' @field hooks
    #' A named list of currently registered callback hooks.
    hooks = function () private$.hooks,
    
    #' @field jobs
    #' Get or set - List of [Jobs][Job] currently managed by this Queue.
    jobs = function (value) q_jobs(private, value),
    
    #' @field state
    #' The Queue's state: `'starting'`, `'idle'`, `'busy'`, `'stopped'`, or `'error.'`
    state = function () private$.state,
    
    #' @field uid
    #' Get or set - Unique identifier, e.g. `'Q1'`.
    uid = function (value) q_uid(private, value),
    
    #' @field tmp
    #' The Queue's temporary directory.
    tmp = function () private$.tmp,
    
    #' @field workers
    #' Get or set - List of [Workers][Worker] used for processing Jobs.
    workers = function (value) q_workers(private, value)
  )
)


q_initialize <- function (
    self, private, 
    globals, packages, init, max_cpus, workers, 
    timeout, hooks, reformat, signal, cpus, 
    stop_id, copy_id, wait ) {
  
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
  private$.tmp      <- normalizePath(tempfile('jqq'), winslash = '/', mustWork = FALSE)
  private$.hooks    <- validate_hooks(hooks[['queue']], 'QH')
  private$max_cpus  <- validate_positive_integer(max_cpus, if_null = availableCores())
  private$n_workers <- validate_positive_integer(workers,  if_null = ceiling(private$max_cpus * 1.2))
  dir.create(private$.tmp)
  
  # Worker configuration
  config_rds_file            <- file.path(private$.tmp, 'config.rds')
  private$w_conf[['config']] <- structure(NA, .jqw_config = config_rds_file)
  private$w_conf[['hooks']]  <- validate_hooks(hooks[['worker']], 'WH')
  saveRDS(file = config_rds_file, list(
    'globals'  = validate_list(globals, if_null = NULL),
    'packages' = validate_character_vector(packages),
    'init'     = validate_expression(init, init_subst) ))
  
  # Job configuration defaults
  private$j_conf[['timeout']]  <- validate_timeout(timeout, func_ok = TRUE)
  private$j_conf[['hooks']]    <- validate_hooks(hooks[['job']], 'JH', func_ok = TRUE)
  private$j_conf[['signal']]   <- validate_character_vector(signal, func_ok = TRUE, bool_ok = TRUE)
  private$j_conf[['cpus']]     <- validate_positive_integer(cpus, func_ok = TRUE, if_null = 1L)
  private$j_conf[['reformat']] <- validate_function(reformat)
  private$j_conf[['stop_id']]  <- validate_function(stop_id)
  private$j_conf[['copy_id']]  <- validate_function(copy_id)
  
  private$set_state('starting')
  later(private$poll_startup)
  
  if (is_true(wait))         self$wait()
  else if (is.numeric(wait)) self$wait(timeout = wait)
  
  if (is_true(private$.is_done))
    cli_abort('Unable to start Queue')
  
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
  
  private$is_ready <- FALSE
  private$.is_done <- TRUE
  
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
  
  else {  # Start more workers
    
    n <- min(
      private$n_workers - length(states), 
      private$max_cpus - sum(states != 'idle') )
    
    for (i in integer(n)) {
      
      worker <- Worker$new(
        hooks   = private$w_conf[['hooks']],
        globals = private$w_conf[['config']],
        wait    = FALSE )
      
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
