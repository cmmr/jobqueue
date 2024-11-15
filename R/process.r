
# Function that runs on the background process

p__start <- function (wd = commandArgs(TRUE), testing = FALSE) {
  
  cnd <- rlang::catch_cnd(
    classes = 'error', 
    expr    = {
      
      Sys.setenv(
        CYGWIN      = "nodosfilewarning",
        R_TESTS     = "",
        R_BROWSER   = "false",
        R_PDFVIEWER = "false" )
      
      wd <- normalizePath(wd, winslash = '/', mustWork = TRUE)
      fp <- function (path) file.path(wd, path)
      
      ps <- ps::ps_handle()
      ps_info = list(
        pid  = ps::ps_pid(ps), 
        time = ps::ps_create_time(ps) )
      saveRDS(ps_info, fp('_ps_info.rds'))
      file.rename(fp('_ps_info.rds'), fp('ps_info.rds'))
      
      env       <- new.env(parent = .GlobalEnv)
      config    <- readRDS(fp('config.rds'))
      semaphore <- basename(wd)
      
      # Packages for Worker
      for (i in seq_along(p <- config[['packages']]))
        require(package = p[[i]], character.only = TRUE)
      
      # Globals for Worker
      for (i in seq_along(g <- config[['globals']])) {
        if (is.function(g[[i]]))
          if (!rlang::is_namespace(environment(g[[i]])))
            environment(g[[i]]) <- env
        assign(x = names(g)[[i]], value = g[[i]], pos = env)
      }
      
      # Init Expression for Worker
      if (!is.null(i <- config[['init']]))
        eval(expr = i, envir = env, enclos = env)
      
    }
  )
  
  
  if (!is.null(cnd)) {
    
    saveRDS(cnd, fp('_error.rds'))
    file.rename(fp('_error.rds'), fp('error.rds'))
    
    if (testing) return (NULL)
    quit(save = "no")
  }
  
  
  file.create(fp('_ready_'))
  
  # Evaluation loop
  repeat {
    
    # Wait for semaphore and request
    request_fp <- fp('request.rds')
    while (!file.exists(request_fp)) {
      semaphore::decrement_semaphore(semaphore, wait = TRUE)
    }
    
    # Evaluate the Job.
    cnd <- rlang::catch_cnd(
      classes = 'error', 
      expr    = {
        
        request <- readRDS(request_fp)
        unlink(request_fp)
        
        Sys.setenv(RCPP_PARALLEL_NUM_THREADS = request$cpus)
        
        output <- eval(
          expr   = request$expr, 
          envir  = list2env(request$vars, parent = env), 
          enclos = baseenv() )
      })
    
    # Return a signaled error instead of output
    if (!is.null(cnd)) output <- cnd
    
    saveRDS(output, fp('_output.rds'))
    file.rename(fp('_output.rds'), fp('output.rds'))
    if (testing) return (NULL)
  }
  
}
