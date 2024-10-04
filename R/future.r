#' 
#' # Imports:
#' #   fastmap,
#' #   future,
#' # 
#' # importFrom("fastmap", "fastmap", "key_missing")
#' # importFrom("future", 
#' #            "%<-%", "future", "Future", "FutureError", "FutureResult", 
#' #            "getExpression", "getGlobalsAndPackages", "nbrOfFreeWorkers", "nbrOfWorkers", "plan", 
#' #            "resolved", "result", "run", "value" )
#' # importFrom("rlang", "env_bind")
#' # 
#' # export(`jobqueue`)
#' # export(`default_queue`)
#' # 
#' # export(`%<-%`)
#' # export(`future`)
#' # export(`nbrOfFreeWorkers`)
#' # export(`nbrOfWorkers`)
#' # export(`plan`)
#' # export(`resolved`)
#' # export(`value`)
#' # 
#' # S3method(resolved,   JobQueueFuture)
#' # S3method(result,     JobQueueFuture)
#' # S3method(run,        JobQueueFuture)
#' # S3method(nbrOfWorkers,     JobQueuePlan)
#' # S3method(nbrOfFreeWorkers, JobQueuePlan)
#' 
#' 
#' 
#' #' jobqueue futures
#' #'
#' #' A jobqueue future is an asynchronous multisession
#' #' future that will be evaluated in a background R session.
#' #'
#' #' @inheritParams JobQueueFuture
#' #' @inheritParams future::future
#' #'
#' #' @return An object of class [JobQueueFuture].
#' #'
#' #' @details
#' #' jobqueue futures rely on the \pkg{callr} package, which is supported on all 
#' #' operating systems.
#' #'
#' #' @export
#' #' @examples
#' #' 
#' #' library(jobqueue)
#' #' plan(jobqueue)
#' #' 
#' #' v %<-% {
#' #'   cat("Hello world!\n")
#' #'   3.14
#' #' }
#' #' v
#' #' 
#' #' f <- future({
#' #'   cat("Hello world!\n")
#' #'   3.14
#' #' })
#' #' v <- value(f)
#' #' v
#' #' 
#' jobqueue <- function(
#'     expr, envir = parent.frame(), substitute = TRUE, globals = TRUE, ...) {
#'   
#'   if (is_true(substitute))
#'     expr <- base::substitute(expr, envir)
#'   
#'   future <- JobQueueFuture(
#'     expr       = expr, 
#'     envir      = envir, 
#'     substitute = FALSE, 
#'     globals    = globals, 
#'     ... )
#'   
#'   if (!future$lazy)
#'     future <- run(future)
#'   
#'   return (future)
#' }
#' 
#' class(jobqueue) <- c('JobQueuePlan', class(jobqueue))
#' attr(jobqueue, "tweakable") <- c('tmax', 'hooks', 'stop_id', 'copy_id', 'cpus', 'queue')
#' 
#' 
#' future_by_uid <- fastmap()
#' 
#' 
#' #' A JobQueueFuture-based future task queue implementation
#' #'
#' #' Set up the future parameters.
#' #' 
#' #' @name JobQueueFuture-class
#' #'
#' #' @inheritParams future::`Future-class`
#' #' @inheritParams future::`Queue-class`
#' #' 
#' #' @param queue  Which [Queue] to run this job on.
#' #'
#' #' @return An object of class `JobQueueFuture`.
#' #' 
#' #' @export
#' JobQueueFuture <- function(
#'     expr       = NULL,
#'     substitute = TRUE,
#'     envir      = parent.frame(),
#'     globals    = TRUE,
#'     packages   = NULL,
#'     lazy       = FALSE,
#'     ... ) {
#'   
#'   if (is_true(substitute)) expr <- base::substitute(expr, envir)
#'   
#'   # See which globals and packages the expression needs.
#'   gp <- getGlobalsAndPackages(
#'     expr       = expr, 
#'     envir      = envir, 
#'     persistent = TRUE, 
#'     globals    = globals )
#'   
#'   future <- Future(
#'     expr       = gp[['expr']], 
#'     substitute = substitute,
#'     envir      = envir,
#'     globals    = c(globals,  gp[['globals']]),
#'     packages   = c(packages, gp[['packages']]),
#'     lazy       = lazy,
#'     ... )
#'   
#'   future_uid <- increment_uid('F')
#'   future_by_uid$set(future_uid, future)
#'   
#'   future$job            <- NULL
#'   future$started        <- Sys.time()
#'   future$job_future_uid <- future_uid
#'   
#'   future$stop <- function (reason = 'future stopped by user') {
#'     if (!inherits(reason, 'condition')) reason %<>% interrupted()
#'     f__assign_result(future_uid, result = reason)
#'   }
#'   
#'   class(future) <- c("JobQueueFuture", class(future))
#'   
#'   return (invisible(future))
#' }
#' 
#' 
#' 
#' #' Start processing a future task.
#' #' @param future  A [JobQueueFuture] object, such as from `JobQueueFuture()`.
#' #' @return The same [JobQueueFuture], invisibly.
#' #' @keywords internal
#' #' @export
#' run.JobQueueFuture <- function(future, ...) {
#'   
#'   if (future$state != "created") return (invisible(future))
#'   
#'   queue <- future[['queue']] %||% default_queue()
#'   if (!inherits(queue, 'Queue'))
#'     cli_abort('`queue` cannot be {.type {queue}}.')
#'   
#'   # Prevent global variable collisions on workers.
#'   globals <- future[['globals']]
#'   if (length(x <- intersect(names(globals), queue$loaded$globals)) > 0)
#'     cli_abort(c(
#'       'x' = "Global variables are already defined on queue's workers: {.var {x}}.",
#'       'i' = 'Use future(globals = list()) or future(globals = FALSE).' ))
#'   
#'   # No package loading after worker initialization.
#'   packages <- unique(future[['packages']])
#'   if (length(x <- setdiff(packages, queue$loaded$packages)) > 0)
#'     cli_abort(c(
#'       'x' = "Packages are not loaded on queue's workers: {x}.",
#'       'i' = 'Specify required packages with Queue$new(packages = ...)',
#'       'i' = 'Or, invoke functions with ::, e.g. stats::mean().' ))
#'   
#'   future$state <- "running"
#'   future$job   <- queue$run(
#'     expr     = getExpression(future),
#'     vars     = globals,
#'     tmax     = future[['tmax']],
#'     hooks    = future[['hooks']],
#'     reformat = future[['reformat']],
#'     cpus     = future[['cpus']],
#'     stop_id  = future[['stop_id']],
#'     copy_id  = future[['copy_id']] )
#'   
#'   future_uid <- future[['job_future_uid']]
#'   future_by_uid$set(future_uid, future)
#'   
#'   future$job$on('done', function (job) f__assign_result(future_uid, job$result))
#'   
#'   return (invisible(future))
#' }
#' 
#' 
#' #' Check on the status of a future task.
#' #' @param x  A [JobQueueFuture] object, such as from `JobQueueFuture()`.
#' #' @return boolean indicating the task is finished (TRUE) or not (FALSE)
#' #' @keywords internal
#' #' @export
#' resolved.JobQueueFuture <- function (x, ...) {
#'   future <- x
#'   if (future$state == "created") run(future)
#'   return (future$state == "finished")
#' }
#' 
#' 
#' #' Return the result of a future task (blocking).
#' #' @return The result of `expr`.
#' #' @keywords internal
#' #' @export
#' result.JobQueueFuture <- function(future, ...) {
#'   if (future$state == "created") run(future)
#'   while (future$state != "finished") run_now(timeoutSecs = 0.2)
#'   return (future$result)
#' }
#' 
#' 
#' 
#' 
#' f__assign_result <- function (future_uid, result) {
#'   
#'   future <- future_by_uid$get(future_uid)
#'   if (is_null(future)) { return (invisible(NULL))         }
#'   else                 { future_by_uid$remove(future_uid) }
#'   
#'   cli_text('Future state changing: {.val {future$state}} -> {.val finished}')
#'   
#'   if (!inherits(result, 'FutureResult'))
#'     result %<>% FutureResult(started = future[['started']])
#'   
#'   cli_text('Assigning result to future: {.type {result}}')
#'   
#'   future$result <- result
#'   future$state  <- "finished"
#'   
#'   cli_text('Future state is: {.val {future$state}}')
#'   
#'   job <- future[['job']]
#'   if (!is_null(job) && !job$is_done) {
#'     cli_text('Calling $stop() on job {job$uid}.')
#'     job$stop('future terminated before job finished')
#'   }
#'   
#'   return (invisible(NULL))
#' }
#' 
#' 
#' 
#' 
#' #' @export
#' nbrOfWorkers.JobQueuePlan <- function (evaluator) { +Inf }
#' 
#' #' @export
#' nbrOfFreeWorkers.JobQueuePlan <- function (evaluator) { +Inf }
#' 
#' 
#' 
#' 
#' #' Get or set the default Queue.
#' #' 
#' #' If a `queue` argument is given, it is set as the new default [Queue] 
#' #' and returned invisibly. Otherwise, the default [Queue] is returned.
#' #' 
#' #' @section Ways a default Queue is set:
#' #' \enumerate{
#' #'    \item{An explicit call to `default_queue(queue = x)`.}
#' #'    \item{When a [Queue] is created with `Queue$new(default = TRUE)`.}
#' #'    \item{`default_queue()` assigns itself `Queue$new()` if needed.}
#' #' }
#' #' 
#' #' Changing the default [Queue] does not shut down the previous one. Like 
#' #' most R objects however, it will eventually be garbage collected if no more 
#' #' references to it exist.
#' #' 
#' #' @param queue  Optional. A [Queue] object or `NULL`.
#' #' 
#' #' @return
#' #'   * `default_queue()` - A [Queue] object.
#' #'   * `default_queue(x)` - `x`, invisibly, which may be a [Queue] object 
#' #'   or `NULL`.
#' #' 
#' #' @export
#' #' @examples
#' #' 
#' #' default_queue()
#' #' default_queue()$shutdown()
#' #' 
#' #' q <- Queue$new(max_cpus = 1)
#' #' default_queue(q)
#' #' v %<-% { paste('Hello', 'world!') }
#' #' v
#' #' q
#' #' q$shutdown()
#' #' 
#' #' Queue$new(workers = 1, default = TRUE)
#' #' default_queue()
#' #' 
#' default_queue <- function (queue) {
#'   
#'   if (missing(queue)) {
#'     q <- env_get(.jobqueue_env, 'default_queue', NULL)
#'     if (is_null(q)) q <- Queue$new(default = TRUE)
#'     return (q)
#'   }
#'   
#'   if (!(is_null(queue) || inherits(queue, 'Queue')))
#'     cli_abort('`queue` must be a Queue object or NULL, not {.type {queue}}.')
#'   
#'   env_bind(.jobqueue_env, 'default_queue' = queue)
#'   
#'   return (invisible(queue))
#' }
