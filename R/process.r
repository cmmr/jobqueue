
# Function that runs on the background process

p__start <- function (tmp = commandArgs(TRUE), testing = FALSE) {
  
  cnd <- rlang::catch_cnd(
    classes = 'error', 
    expr    = {
      
      # Protect from attached packages
      output <- with(
        data = list2env(list(tmp = tmp, testing = testing), parent = baseenv()),
        expr = {
      
          Sys.setenv(
            CYGWIN      = "nodosfilewarning",
            R_TESTS     = "",
            R_BROWSER   = "false",
            R_PDFVIEWER = "false" )
          
          fp         <- function (x) file.path(tmp, x)
          ready_fp   <- fp('_ready_')
          request_fp <- fp('request.rds')
          
          semaphore  <- semaphore::create_semaphore()
          ps         <- ps::ps_handle()
          parent     <- ps::ps_parent(ps)
          ps_info    <- list(
            pid  = ps::ps_pid(ps), 
            time = ps::ps_create_time(ps),
            tmpd = tempdir(),
            sem  = semaphore )
          saveRDS(ps_info, fp('_ps_info.rds'))
          file.rename(fp('_ps_info.rds'), fp('ps_info.rds'))
          
          
          # Environment for evaluating Jobs:
          worker_env <- new.env(parent = globalenv())
          config     <- readRDS(fp('config.rds'))
          if (is.character(config)) config <- readRDS(config)
          
          ## Global variables
          for (i in seq_along(g <- config[['globals']])) {
            if (is.function(g[[i]]) && !isNamespace(environment(g[[i]])))
              environment(g[[i]]) <- worker_env
            assign(x = names(g)[[i]], value = g[[i]], pos = worker_env)
          }
          
          ## Packages
          for (pkg in unique(config[['packages']]))
            if (!require(package = pkg, character.only = TRUE, quietly = TRUE, warn.conflicts = FALSE))
              stop('Could not load package: ', pkg, call. = FALSE)
          
          ## Namespace
          if (!is.null(config[['namespace']]))
            parent.env(worker_env) <- asNamespace(config[['namespace']])
          
          ## Init Expression
          if (!is.null(config[['init']]))
            eval(expr = config[['init']], envir = worker_env)
          
          
          file.create(ready_fp)
          
          if (!isFALSE(testing)) {
            semaphore::increment_semaphore(semaphore)
            if (is.function(testing)) {
              environment(testing) <- environment()
              testing(tmp)
            }
            testing <- TRUE
          }
          
          
          # Evaluation loop
          repeat {
            
            
            # Wait for semaphore; check if abandoned
            repeat {
              if (!file.exists(ready_fp))     stop('ready file missing', call. = FALSE)
              if (!ps::ps_is_running(parent)) stop('parent exited',      call. = FALSE) # nocov
              if (semaphore::decrement_semaphore(semaphore, wait = 2)) break
            }
            
            
            # Evaluate the Job.
            cnd <- rlang::catch_cnd(
              classes = 'error', 
              expr    = {
                
                request <- readRDS(request_fp)
                unlink(request_fp)
                
                Sys.setenv(RCPP_PARALLEL_NUM_THREADS = request$cpus)
                
                expr  <- request$expr
                envir <- list2env(request$vars, parent = worker_env)
                
                options(rlang_trace_top_env = envir)
                
                output <- eval(expr, envir)
              })
            
            # Return a signaled error instead of output
            if (!is.null(cnd)) output <- cnd
            
            saveRDS(output, fp('_output.rds'))
            file.rename(fp('_output.rds'), fp('output.rds'))
            if (testing) return (output)
          }
          
      })
      
  })
  
  
  fp <- function (x) file.path(tmp, x)
  saveRDS(cnd, fp('_error.rds'))
  file.rename(fp('_error.rds'), fp('error.rds'))
  
  if (!is.null(cnd))           output <- cnd
  if (!base::isFALSE(testing)) return (output)
  stop('shutting down subprocess', call. = FALSE)
  base::quit(save = 'no', status = 1L, runLast = FALSE) # nocov
}
