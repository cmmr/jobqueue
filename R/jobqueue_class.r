
#' Assigns Jobs to Workers (R6 Class)
#'
#' @description
#' 
#' Jobs go in. Results come out.
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
#' @param expr  A call or R expression wrapped in curly braces to evaluate on a 
#'        [`worker`][worker_class]. Will have access to any variables defined 
#'        by `vars`, as well as the [`jobqueue's`][jobqueue_class] `globals`, 
#'        `packages`, and `init` configuration. See `vignette('eval')`.
#' 
#' @param vars  A named list of variables to make available to `expr` during 
#'        evaluation. Alternatively, an object that can be coerced to a named 
#'        list with `as.list()`, e.g. named vector, data.frame, or environment. 
#'        Or a `function (job)` that returns such an object.
#' 
#' @param timeout  A named numeric vector indicating the maximum number of 
#'        seconds allowed for each state the [`job`][job_class] passes through, 
#'        or 'total' to apply a single timeout from 'submitted' to 'done'. Can 
#'        also limit the 'starting' state for [`workers`][worker_class]. A 
#'        `function (job)` can be used in place of a number.
#'        Example: `timeout = c(total = 2.5, running = 1)`. 
#'        See `vignette('stops')`.
#'        
#' @param hooks  A named list of functions to run when the [`job`][job_class] 
#'        state changes, of the form 
#'        `hooks = list(created = function (worker) {...})`. Or a 
#'        `function (job)` that returns the same. Names of 
#'        [`worker`][worker_class] hooks are typically `'created'`, 
#'        `'submitted'`, `'queued'`, `'dispatched'`, `'starting'`, `'running'`, 
#'        `'done'`, or `'*'` (duplicates okay). See `vignette('hooks')`.
#'        
#' @param reformat  Set `reformat = function (job)` to define what 
#'        `<job>$result` should return. The default, `reformat = NULL` passes 
#'        `<job>$output` to `<job>$result` unchanged.
#'        See `vignette('results')`.
#'        
#' @param signal  Should calling `<job>$result` signal on condition objects?
#'        When `FALSE`, `<job>$result` will return the object without 
#'        taking additional action. Setting to `TRUE` or a character vector of 
#'        condition classes, e.g. `c('interrupt', 'error', 'warning')`, will
#'        cause the equivalent of `stop(<condition>)` to be called when those
#'        conditions are produced. Alternatively, a `function (job)` that 
#'        returns `TRUE` or `FALSE`. See `vignette('results')`.
#'        
#' @param cpus  How many CPU cores to reserve for this [`job`][job_class]. Or a 
#'        `function (job)` that returns the same. Used to limit the number of 
#'        [`jobs`][job_class] running simultaneously to respect 
#'        `<jobqueue>$max_cpus`. Does not prevent a [`job`][job_class] from 
#'        using more CPUs than reserved.
#' 
#' @param max_cpus  Total number of CPU cores that can be reserved by all 
#'        running [`jobs`][job_class] (`sum(<job>$cpus)`). Does not enforce 
#'        limits on actual CPU utilization.
#'        
#' @param workers  How many background [`worker`][worker_class] processes to 
#'        start. Set to more than `max_cpus` to enable standby 
#'        [`workers`][worker_class] to quickly swap out with 
#'        [`workers`][worker_class] that need to restart.
#'        
#' @param job  A [`job`][job_class] object, as created by `job_class$new()`.
#'        
#' @param stop_id  If an existing [`job`][job_class] in the 
#'        [`jobqueue`][jobqueue_class] has the same `stop_id`, that 
#'        [`job`][job_class] will be stopped and return an 'interrupt' 
#'        condition object as its result. `stop_id` can also be a 
#'        `function (job)` that returns the `stop_id` to assign to a given 
#'        [`job`][job_class]. A `stop_id` of `NULL` disables this feature. 
#'        See `vignette('stops')`.
#'                 
#' @param copy_id  If an existing [`job`][job_class] in the 
#'        [`jobqueue`][jobqueue_class] has the same `copy_id`, the newly 
#'        submitted [`job`][job_class] will become a "proxy" for that earlier 
#'        [`job`][job_class], returning whatever result the earlier 
#'        [`job`][job_class] returns. `copy_id` can also be a `function (job)` 
#'        that returns the `copy_id` to assign to a given [`job`][job_class]. 
#'        A `copy_id` of `NULL` disables this feature. 
#'        See `vignette('stops')`.
#'        
#' @param reason  Passed to `<job>$stop()` for any [`jobs`][job_class] 
#'        currently managed by this [`jobqueue`][jobqueue_class].
#'        
#' @param cls  Passed to `<job>$stop()` for any [`jobs`][job_class] currently 
#'        managed by this [`jobqueue`][jobqueue_class].
#'        
#' @param state
#' The name of a [`jobqueue`][jobqueue_class] state. Typically one of:
#' 
#' * `'*'` -        Every time the state changes.
#' * `'.next'` -    Only one time, the next time the state changes.
#' * `'starting'` - [`workers`][worker_class] are starting.
#' * `'idle'` -     All [`workers`][worker_class] are ready/idle.
#' * `'busy'` -     At least one [`worker`][worker_class] is busy.
#' * `'stopped'` -  Shutdown is complete.
#'        
#' @param func  A function that accepts a [`jobqueue`][jobqueue_class] object 
#'        as input. Return value is ignored.
#'        
#' @param ...  Arbitrary named values to add to the returned [`job`][job_class] 
#'        object.
#' 
#' @export

jobqueue_class <- R6Class(
  classname    = "jobqueue",
  cloneable    = FALSE,
  lock_objects = FALSE,

  public = list(

    
    #' @description
    #' Creates a pool of background processes for handling `$run()` and 
    #' `$submit()` calls. These [`workers`][worker_class] are initialized 
    #' according to the `globals`, `packages`, and `init` arguments.
    #'
    #' @param timeout,hooks,reformat,signal,cpus,stop_id,copy_id
    #'        Defaults for this [`jobqueue's`][jobqueue_class] `$run()` method. 
    #'        Here only, `stop_id` and `copy_id` must be either a 
    #'        `function (job)` or `NULL`. `hooks` can set 
    #'        [`jobqueue`][jobqueue_class], [`worker`][worker_class], and/or 
    #'        [`job`][job_class] hooks - see the "Attaching" section in 
    #'        `vignette('hooks')`.
    #'
    #' @return A [`jobqueue`][jobqueue_class] object.
    initialize = function (
        globals   = NULL,
        packages  = NULL,
        namespace = NULL,
        init      = NULL,
        max_cpus  = availableCores(),
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
        globals, packages, namespace, init, max_cpus, workers, 
        timeout, hooks, reformat, signal, cpus, 
        stop_id, copy_id )
    },
    
    
    #' @description
    #' Print method for a [`jobqueue`][jobqueue_class].
    #' @param ... Arguments are not used currently.
    print = function (...) q_print(self, private),
    
    
    #' @description
    #' Creates a [`job`][job_class] object and submits it to the 
    #' [`jobqueue`][jobqueue_class] for running. Any `NA` arguments will be 
    #' replaced with their value from `jobqueue_class$new()`.
    #'
    #' @return The new [`job`][job_class] object.
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
    #' Adds a [`job`][job_class] to the [`jobqueue`][jobqueue_class] for 
    #' running on a background process.
    #' @return This [`jobqueue`][jobqueue_class], invisibly.
    submit = function (job)
      q_submit(self, private, job),
    
    #' @description
    #' Blocks until the [`jobqueue`][jobqueue_class] enters the given state.
    #' @param timeout Stop the [`jobqueue`][jobqueue_class] if it takes longer 
    #'        than this number of seconds, or `NULL`.
    #' @param signal Raise an error if encountered (will also be recorded in 
    #'        `<jobqueue>$cnd`).
    #' @return This [`jobqueue`][jobqueue_class], invisibly.
    wait = function (state = 'idle', timeout = NULL, signal = TRUE) 
      u_wait(self, private, state, timeout, signal),
    
    #' @description
    #' Attach a callback function to execute when the 
    #' [`jobqueue`][jobqueue_class] enters `state`.
    #' @return A function that when called removes this callback from the 
    #'         [`jobqueue`][jobqueue_class].
    on = function (state, func) 
      u_on(self, private, 'QH', state, func),
    
    #' @description
    #' Stop all [`jobs`][job_class] and [`workers`][worker_class].
    #' @return This [`jobqueue`][jobqueue_class], invisibly.
    stop = function (reason = 'jobqueue shut down by user', cls = NULL) {
      q_stop(self, private, reason, cls)
    }
  ),


  private = list(
    
    finalize = function (reason = 'jobqueue was garbage collected', cls = NULL) {
      
      fmap(private$.workers, 'stop', reason, cls)
      fmap(private$.jobs,    'stop', reason, cls)
      
      try(silent = TRUE, dir_delete(private$q_dir))
      
      return (invisible(NULL))
    },
    
    .hooks     = list(),
    .jobs      = list(),
    .workers   = list(),
    .state     = 'initializing',
    .cnd       = NULL,
    .is_done   = FALSE,
    
    n_workers  = NULL,
    up_since   = NULL,
    total_runs = 0L,
    is_ready   = FALSE,
    j_conf     = list(),
    w_conf     = list(),
    max_cpus   = NULL,
    q_dir      = NULL,
    
    set_state  = function (state) u__set_state(self, private, state),
    dispatch   = function (...)   q__dispatch(self, private)
  ),
  
  active = list(
    
    #' @field hooks
    #' A named list of currently registered callback hooks.
    hooks = function () private$.hooks,
    
    #' @field jobs
    #' Get or set - List of [jobs][job_class] currently managed by this 
    #' [`jobqueue`][jobqueue_class].
    jobs = function (value) q_jobs(private, value),
    
    #' @field state
    #' The [`jobqueue's`][jobqueue_class] state: `'starting'`, `'idle'`, 
    #' `'busy'`, `'stopped'`, or `'error.'`
    state = function () private$.state,
    
    #' @field uid
    #' A short string, e.g. `'Q1'`, that uniquely identifies this 
    #' [`jobqueue`][jobqueue_class].
    uid = function () basename(private$q_dir),
    
    #' @field tmp
    #' The [`jobqueue`][jobqueue_class]'s temporary directory.
    tmp = function () private$q_dir,
    
    #' @field workers
    #' Get or set - List of [`workers`][worker_class] used for processing 
    #' [`jobs`][job_class].
    workers = function (value) q_workers(private, value),
    
    #' @field cnd
    #' The error that caused the [`jobqueue`][jobqueue_class] to stop.
    cnd = function () private$.cnd
  )
)


q_initialize <- function (
    self, private, 
    globals, packages, namespace, init, max_cpus, workers, 
    timeout, hooks, reformat, signal, cpus, 
    stop_id, copy_id ) {
  
  init_subst       <- substitute(init, env = parent.frame())
  private$up_since <- Sys.time()
  
  timeout <- validate_timeout(timeout, func_ok = TRUE)
  
  # Assign hooks by q_ and w_ prefixes
  hooks <- validate_list(hooks)
  if (!all(names(hooks) %in% c('queue', 'worker', 'job')))
    hooks <- list(
      job    = hooks[grep('^[qwj]_', names(hooks), invert = TRUE)], 
      queue  = hooks[grep('^q_',     names(hooks))], 
      worker = hooks[grep('^w_',     names(hooks))] )
  hooks[['worker']] <- c(list('idle' = private$dispatch), hooks[['worker']])
  
  # Queue configuration
  private$q_dir     <- dir_create(ENV$jq_dir, 'Q')
  private$.hooks    <- validate_hooks(hooks[['queue']], 'QH')
  private$max_cpus  <- validate_positive_integer(max_cpus, if_null = availableCores())
  private$n_workers <- validate_positive_integer(workers,  if_null = ceiling(private$max_cpus * 1.2))
  
  # Worker configuration
  private$w_conf[['hooks']]   <- validate_hooks(hooks[['worker']], 'WH')
  private$w_conf[['timeout']] <- timeout[['starting']]
  saveRDS(
    file   = file.path(private$q_dir, 'config.rds'), 
    object = list(
    'globals'   = validate_list(globals),
    'packages'  = validate_character_vector(packages),
    'namespace' = validate_string(namespace, null_ok = TRUE),
    'init'      = validate_expression(init, init_subst) ))
  
  # Job configuration defaults
  private$j_conf[['timeout']]  <- timeout[setdiff(names(timeout), 'starting')]
  private$j_conf[['hooks']]    <- validate_hooks(hooks[['job']], 'JH', func_ok = TRUE)
  private$j_conf[['signal']]   <- validate_character_vector(signal, func_ok = TRUE, bool_ok = TRUE)
  private$j_conf[['cpus']]     <- validate_positive_integer(cpus, func_ok = TRUE, if_null = 1L)
  private$j_conf[['reformat']] <- validate_function(reformat)
  private$j_conf[['stop_id']]  <- validate_function(stop_id)
  private$j_conf[['copy_id']]  <- validate_function(copy_id)
  
  private$set_state('starting')
  
  
  
  # Creates `n_workers`, at most `max_cpus` starting at a time.
  
  repeat {
    
    workers <- self$workers
    states  <- map(workers, 'state')
    
    if (length(i <- which(states == 'stopped'))) {
      
      i   <- i[[1]]
      cnd <- as_error(workers[[i]]$cnd)
      self$stop(cnd)
      cnd_signal(cnd)
      stop(cnd$message)
    }
    
    else if (sum(states == 'idle') == private$n_workers) {
      
      private$is_ready <- TRUE
      private$set_state('idle')
      break
    }
    
    else {  # Start more workers
      
      n <- min(
        private$n_workers - length(states), 
        private$max_cpus - sum(states != 'idle') )
      
      for (i in integer(n)) {
        
        worker <- worker_class$new(
          hooks   = private$w_conf[['hooks']],
          globals = self,
          wait    = FALSE,
          timeout = private$w_conf[['timeout']] )
        
        self$workers %<>% c(worker)
      }
      
    }
    
    later::run_now(timeoutSecs = 0.6)
  }
  
  
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
  
  # Replace NA values with defaults from jobqueue_class$new() call.
  j_conf <- private$j_conf
  
  if (is_formula(stop_id)) stop_id <- as_function(stop_id)
  if (is_formula(copy_id)) copy_id <- as_function(copy_id)
  
  job <- job_class$new(
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
  
  job$.trace <- trace_back(bottom = 2L)
  
  self$submit(job)
  
  return (invisible(job))
}


q_submit <- function (self, private, job) {
  
  if (!inherits(job, 'job'))
    cli_abort('`job` must be a job object, not {.type {job}}.')
  
  if (job$is_done) return (invisible(job))
  
  if (is_null(job$.call))  job$.call  <- caller_env(n = 2L)
  if (is_null(job$.trace)) job$.trace <- trace_back(bottom = job$.call)
  
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
  private$.cnd     <- as_cnd(reason, c(cls, 'interrupt'))
  
  private$finalize(reason = private$.cnd)
  private$set_state('stopped')
  self$workers <- list()
  self$jobs    <- list()
  
  return (invisible(self))
}


# Use any idle workers to run queued jobs.
q__dispatch <- function (self, private) {
  
  # Purge jobs that are done.
  self$jobs <- get_eq(self$jobs, 'is_done', FALSE)
  
  # Only start jobs if this jobqueue is ready.
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
  
  # Update `<jobqueue>$state` and trigger callback hooks.
  busy_workers <- get_eq(self$workers, 'state', 'busy')
  queue_state  <- ifelse(length(busy_workers) > 0, 'busy', 'idle')
  private$set_state(state = queue_state)
  
  return (invisible(NULL))
}



# Active bindings to validate new values.

q_jobs <- function (private, value) {
  if (missing(value)) return (private$.jobs)
  private$.jobs <- validate_list(value, of_type = 'job', named = FALSE)
}

q_workers <- function (private, value) {
  if (missing(value)) return (private$.workers)
  private$.workers <- validate_list(value, of_type = 'worker', named = FALSE)
}

