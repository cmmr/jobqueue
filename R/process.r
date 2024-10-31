
# Functions that run on the background process


p__start <- function () {
  
  cnd <- rlang::catch_cnd(classes = "error", {
    
    wd <- commandArgs(trailingOnly = TRUE)
    stopifnot(isTRUE(dir.exists(wd)))
    setwd(dir = wd)
    
    writeLines(as.character(Sys.getpid()), '_pid.txt')
    file.rename('_pid.txt', 'pid.txt')
    
    env <- new.env(parent = .GlobalEnv)
    p__config(env) # Read/apply config.rds
    p__report(env) # Write loaded.rds
    p__listen(env) # Evaluation loop
  })
  
  if (!is.null(cnd)) {
    saveRDS(cnd, 'error.rds')
    quit(save = "no")
  }
}


p__config <- function (env) {
  
  config <- readRDS('config.rds')
  
  # Packages for Worker
  for (i in seq_along(p <- config[['packages']]))
    require(package = p[[i]], character.only = TRUE)
  
  # Globals for Worker
  for (i in seq_along(g <- config[['globals']]))
    assign(x = names(g)[[i]], value = g[[i]], pos = env)
  
  # Init Expression for Worker
  if (!is.null(i <- config[['init']]))
    eval(expr = i, envir = env, enclos = env)
  
  return (env)
}


p__report <- function (env) {
  
  # Globals available for Jobs
  globals <- sort(unique(c(
    ls(env,        all.names = TRUE),
    ls(.GlobalEnv, all.names = TRUE) )))
  
  # Attached objects available for Jobs
  attached <- list()
  invisible(lapply(
    X   = rev(intersect(search(), paste0('package:', loadedNamespaces()))), 
    FUN = function (pkg) {
      for (nm in getNamespaceExports(sub('^package:', '', pkg)))
        attached[[nm]] <<- pkg }))
  
  loaded <- list(globals = globals, attached = attached)
  saveRDS(loaded, 'loaded.rds')
}


p__listen <- function (env) {
  
  file.create('_ready_')
  lockfile <- 'ready.lock'
  
  repeat {
    
    # Pause until Worker unblocks us.
    lck <- filelock::lock(lockfile)
    filelock::unlock(lck)
    unlink(lockfile)
        
    # Import Job details.
    request  <- readRDS('request.rds')
    lockfile <- paste0(request$uid, '.lock')
    
    # Evaluate the Job.
    cnd <- rlang::catch_cnd(classes = "error", {
      output <- eval(
        expr   = request$expr, 
        envir  = request$vars, 
        enclos = env )
    })
    
    # Return any signaled condition instead of output
    if (!is.null(cnd)) output <- cnd
    
    saveRDS(output, '_output.rds')
    file.rename('_output.rds', 'output.rds')
  }
  
}
