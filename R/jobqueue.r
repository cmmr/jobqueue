
#' Assigns Jobs to Workers
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
#' @param max_cpus  Total number of CPU cores that can be reserved by all 
#'        running [`jobs`][job_class] (`sum(<job>$cpus)`). Does not enforce 
#'        limits on actual CPU utilization.
#'        
#' @param workers  How many background [`worker`][worker_class] processes to 
#'        start. Set to more than `max_cpus` to enable standby 
#'        [`workers`][worker_class] to quickly swap out with 
#'        [`workers`][worker_class] that need to restart.
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
#' @param cpus  The default number of CPU cores per [`job`][job_class]. Or a 
#'        `function (job)` that returns the number of CPU cores to reserve for 
#'        a given [`job`][job_class]. Used to limit the number of 
#'        [`jobs`][job_class] running simultaneously to respect 
#'        `<jobqueue>$max_cpus`. Does not prevent a [`job`][job_class] from 
#'        using more CPUs than reserved.
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
#'        A `copy_id` of `NULL` disables this feature. See `vignette('stops')`.
#' 
#' @return
#' A [`jobqueue`][jobqueue_class] object.
#'
#' @export
#' @examplesIf ! jobqueue:::is_cran_check()
#' 
#' jq <- jobqueue(globals = list(N = 42), workers = 2)
#' print(jq)
#' 
#' job <- jq$run({ paste("N is", N) })
#' job$result
#' 
#' jq$stop()

jobqueue <- function (
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
  
  
  # Capture curly-brace expression
  init_subst <- substitute(init)
  init <- validate_expression(init, init_subst)
  
  
  jobqueue_class$new(
    globals   = globals,
    packages  = packages,
    namespace = namespace,
    init      = init,
    max_cpus  = max_cpus,
    workers   = workers,
    timeout   = timeout,
    hooks     = hooks,
    reformat  = reformat,
    signal    = signal,
    cpus      = cpus,
    stop_id   = stop_id,
    copy_id   = copy_id )
  
}
