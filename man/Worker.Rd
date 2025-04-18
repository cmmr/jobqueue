% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/worker.r
\name{Worker}
\alias{Worker}
\title{A Background Process}
\description{
Where \link{Job} expressions are evaluated.
}
\section{Active bindings}{
\if{html}{\out{<div class="r6-active-bindings">}}
\describe{
\item{\code{hooks}}{A named list of currently registered callback hooks.}

\item{\code{job}}{The currently running Job.}

\item{\code{ps}}{The \code{ps::ps_handle()} object for the background process.}

\item{\code{state}}{The Worker's state: \code{'starting'}, \code{'idle'}, \code{'busy'}, or \code{'stopped'}.}

\item{\code{uid}}{A short string, e.g. \code{'W11'}, that uniquely identifies this Worker.}

\item{\code{tmp}}{The Worker's temporary directory.}

\item{\code{cnd}}{The error that caused the Worker to stop.}
}
\if{html}{\out{</div>}}
}
\section{Methods}{
\subsection{Public methods}{
\itemize{
\item \href{#method-Worker-new}{\code{Worker$new()}}
\item \href{#method-Worker-print}{\code{Worker$print()}}
\item \href{#method-Worker-start}{\code{Worker$start()}}
\item \href{#method-Worker-stop}{\code{Worker$stop()}}
\item \href{#method-Worker-restart}{\code{Worker$restart()}}
\item \href{#method-Worker-on}{\code{Worker$on()}}
\item \href{#method-Worker-wait}{\code{Worker$wait()}}
\item \href{#method-Worker-run}{\code{Worker$run()}}
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-Worker-new"></a>}}
\if{latex}{\out{\hypertarget{method-Worker-new}{}}}
\subsection{Method \code{new()}}{
Creates a background R process for running \link{Job}s.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{Worker$new(
  globals = NULL,
  packages = NULL,
  namespace = NULL,
  init = NULL,
  hooks = NULL,
  wait = TRUE,
  timeout = Inf
)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{globals}}{A named list of variables that all \verb{<Job>$expr}s will have
access to. Alternatively, an object that can be coerced to a named
list with \code{as.list()}, e.g. named vector, data.frame, or environment.}

\item{\code{packages}}{Character vector of package names to load on workers.}

\item{\code{namespace}}{The name of a package to attach to the worker's
environment.}

\item{\code{init}}{A call or R expression wrapped in curly braces to evaluate on
each worker just once, immediately after start-up. Will have access
to variables defined by \code{globals} and assets from \code{packages} and
\code{namespace}. Returned value is ignored.}

\item{\code{hooks}}{A named list of functions to run when the Worker state
changes, of the form \code{hooks = list(idle = function (worker) {...})}.
Names of worker hooks are typically \code{starting}, \code{idle}, \code{busy},
\code{stopped}, or \code{'*'} (duplicates okay). See \code{vignette('hooks')}.}

\item{\code{wait}}{If \code{TRUE}, blocks until the Worker is 'idle'. If \code{FALSE}, the
Worker object is returned in the 'starting' state.}

\item{\code{timeout}}{How long to wait for the worker to finish starting (in seconds).
If \code{NA}, defaults to the \code{Worker$new()} argument.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
A Worker object.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-Worker-print"></a>}}
\if{latex}{\out{\hypertarget{method-Worker-print}{}}}
\subsection{Method \code{print()}}{
Print method for a \code{Worker}.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{Worker$print(...)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{...}}{Arguments are not used currently.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
The Worker, invisibly.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-Worker-start"></a>}}
\if{latex}{\out{\hypertarget{method-Worker-start}{}}}
\subsection{Method \code{start()}}{
Restarts a stopped Worker.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{Worker$start(wait = TRUE, timeout = NA)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{wait}}{If \code{TRUE}, blocks until the Worker is 'idle'. If \code{FALSE}, the
Worker object is returned in the 'starting' state.}

\item{\code{timeout}}{How long to wait for the worker to finish starting (in seconds).
If \code{NA}, defaults to the \code{Worker$new()} argument.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
The Worker, invisibly.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-Worker-stop"></a>}}
\if{latex}{\out{\hypertarget{method-Worker-stop}{}}}
\subsection{Method \code{stop()}}{
Stops a Worker by terminating the background process and calling
\verb{<Job>$stop(reason)} on any Jobs currently assigned to this Worker.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{Worker$stop(reason = "worker stopped by user", cls = NULL)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{reason}}{Passed to \verb{<Job>$stop()} for any Jobs currently managed by
this Worker.}

\item{\code{cls}}{Passed to \verb{<Job>$stop()} for any Jobs currently managed by this
Worker.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
The Worker, invisibly.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-Worker-restart"></a>}}
\if{latex}{\out{\hypertarget{method-Worker-restart}{}}}
\subsection{Method \code{restart()}}{
Restarts a Worker by calling \verb{<Worker>$stop(reason)} and
\verb{<Worker>$start()} in succession.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{Worker$restart(
  wait = TRUE,
  timeout = NA,
  reason = "restarting worker",
  cls = NULL
)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{wait}}{If \code{TRUE}, blocks until the Worker is 'idle'. If \code{FALSE}, the
Worker object is returned in the 'starting' state.}

\item{\code{timeout}}{How long to wait for the worker to finish starting (in seconds).
If \code{NA}, defaults to the \code{Worker$new()} argument.}

\item{\code{reason}}{Passed to \verb{<Job>$stop()} for any Jobs currently managed by
this Worker.}

\item{\code{cls}}{Passed to \verb{<Job>$stop()} for any Jobs currently managed by this
Worker.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
The Worker, invisibly.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-Worker-on"></a>}}
\if{latex}{\out{\hypertarget{method-Worker-on}{}}}
\subsection{Method \code{on()}}{
Attach a callback function to execute when the Worker enters \code{state}.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{Worker$on(state, func)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{state}}{The name of a Worker state. Typically one of:
\itemize{
\item \code{'*'} -        Every time the state changes.
\item \code{'.next'} -    Only one time, the next time the state changes.
\item \code{'starting'} - Waiting for the background process to load.
\item \code{'idle'} -     Waiting for Jobs to be \verb{$run()}.
\item \code{'busy'} -     While a Job is running.
\item \code{'stopped'} -  After \verb{<Worker>$stop()} is called.
}}

\item{\code{func}}{A function that accepts a Worker object as input. You can call
\verb{<Worker>$stop()} and other \verb{<Worker>$} methods.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
A function that when called removes this callback from the Worker.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-Worker-wait"></a>}}
\if{latex}{\out{\hypertarget{method-Worker-wait}{}}}
\subsection{Method \code{wait()}}{
Blocks until the Worker enters the given state.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{Worker$wait(state = "idle", timeout = Inf, signal = TRUE)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{state}}{The name of a Worker state. Typically one of:
\itemize{
\item \code{'*'} -        Every time the state changes.
\item \code{'.next'} -    Only one time, the next time the state changes.
\item \code{'starting'} - Waiting for the background process to load.
\item \code{'idle'} -     Waiting for Jobs to be \verb{$run()}.
\item \code{'busy'} -     While a Job is running.
\item \code{'stopped'} -  After \verb{<Worker>$stop()} is called.
}}

\item{\code{timeout}}{Stop the Worker if it takes longer than this number of seconds.}

\item{\code{signal}}{Raise an error if encountered (will also be recorded in \verb{<Worker>$cnd}).}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
This Worker, invisibly.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-Worker-run"></a>}}
\if{latex}{\out{\hypertarget{method-Worker-run}{}}}
\subsection{Method \code{run()}}{
Assigns a Job to this Worker for evaluation on the background
process.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{Worker$run(job)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{job}}{A \link{Job} object, as created by \code{Job$new()}.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
This Worker, invisibly.
}
}
}
