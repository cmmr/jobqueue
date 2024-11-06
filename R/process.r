
# Function that runs on the background process

p__start <- function () {
  
  env       <- new.env(parent = .GlobalEnv)
  semaphore <- NULL
  
  cnd <- rlang::catch_cnd(
    classes = 'error', 
    expr    = local({
      
      setwd(commandArgs(TRUE))
      config     <- readRDS('config.rds')
      semaphore <<- readRDS('semaphore.rds')
      
      # Packages for Worker
      for (i in seq_along(p <- config[['packages']]))
        require(package = p[[i]], character.only = TRUE)
      
      # Globals for Worker
      for (i in seq_along(g <- config[['globals']]))
        assign(x = names(g)[[i]], value = g[[i]], pos = env)
      
      # Init Expression for Worker
      if (!is.null(i <- config[['init']]))
        eval(expr = i, envir = env, enclos = env)
    })
  )
  
  if (is.null(cnd)) {
    ps      <- ps::ps_handle()
    ps_info <- list(pid = ps::ps_pid(ps), time = ps::ps_create_time(ps))
    saveRDS(ps_info, '_ps_info.rds')
    file.rename('_ps_info.rds', 'ps_info.rds')
  } else {
    saveRDS(cnd, '_error.rds')
    file.rename('_error.rds', 'error.rds')
    quit(save = "no")
  }
  
  # Evaluation loop
  repeat {
    
    # Wait for semaphore
    while(!file.exists('request.rds')) {
      semaphore::decrement_semaphore(semaphore, wait = TRUE)
    }
    
    # Evaluate the Job.
    cnd <- rlang::catch_cnd(
      classes = 'error', 
      expr    = {
        request <- readRDS('request.rds')
        unlink('request.rds')
        output  <- eval(
          expr   = request$expr, 
          envir  = request$vars, 
          enclos = env )
      })
    
    # Return a signaled error instead of output
    if (!is.null(cnd)) output <- cnd
    
    saveRDS(output, '_output.rds')
    file.rename('_output.rds', 'output.rds')
  }
  
}
