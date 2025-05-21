
# Function that runs on a background process

p__start <- function (ppid, mqid, p_dir, config) {
  
  
  setup_vars <- local({
    
    Sys.setenv(
      CYGWIN      = "nodosfilewarning",
      R_TESTS     = "",
      R_BROWSER   = "false",
      R_PDFVIEWER = "false" )
    
    # Report our ps_string and semaphore to the parent process
    pid <- p__ps_string()
    sem <- interprocess::semaphore()
    file_create(p_dir, c('pid_', pid))
    file_create(p_dir, c('sem_', sem$name))
    
    # Help the monitor terminate this process tree
    mq <- interprocess::msg_queue(mqid, assert = 'exists')
    mq$send(paste(pid, sem$name, dir_create(tempdir())))
    do.call(Sys.setenv, as.list(structure('YES', names = sem$name)))
    do.call(Sys.setenv, as.list(structure('YES', names = mq$name)))
    
    list(
      p_dir  = p_dir,
      parent = p__ps_handle(ppid),
      config = readRDS(config),
      sem    = sem )
  })
  
  
  # `setup_env` isn't effected by user's configuration.
  setup_env <- list2env(setup_vars, parent = baseenv())
  remove(list = setdiff(ls(), 'setup_env'))
  
  
  with(
    data = setup_env,
    expr = {
      
      # `eval_env` persists user's configuration.
      eval_env <- new.env(parent = globalenv())
      
      local({
      
        ## Global variables
        for (i in seq_along(g <- config[['globals']])) {
          val <- g[[i]]
          if (is.function(val) && !isNamespace(environment(val)))
            environment(val) <- eval_env
          assign(x = names(g)[[i]], value = val, pos = eval_env)
        }
        
        ## Packages
        for (pkg in unique(config[['packages']]))
          if (!require(package = pkg, character.only = TRUE, quietly = TRUE, warn.conflicts = FALSE))
            stop('Could not load package: ', pkg, call. = FALSE)
        
        ## Namespace
        if (!is.null(config[['namespace']]))
          parent.env(eval_env) <- asNamespace(config[['namespace']])
        
        ## Init Expression
        if (!is.null(config[['init']]))
          eval(expr = config[['init']], envir = eval_env)
        
      })
      lockEnvironment(eval_env, bindings = TRUE)
      
      
      # Creating 'ready' tells the worker that we're ready for jobs.
      ready_file <- file.path(p_dir, 'ready')
      file.create(ready_file)
      
      
      # Evaluation loop
      repeat {
        
        # Wait for semaphore; check if abandoned
        repeat {
          if (!file.exists(ready_file))    stop('ready file deleted', call. = FALSE)
          if (!ps::ps_is_running(parent))  stop('parent exited',      call. = FALSE)
          if (sem$wait(timeout_ms = 2000)) break
        }
        
        local({
          
          # Evaluate the Job.
          cnd <- rlang::catch_cnd(
            classes = 'error', 
            expr    = {
              
              request <- readRDS(file.path(p_dir, 'request.rds'))
              expr    <- request$expr
              envir   <- list2env(request$vars, parent = eval_env)
              
              options(rlang_trace_top_env = envir)
              Sys.setenv(RCPP_PARALLEL_NUM_THREADS = request$cpus)
              
              output <- eval(expr, envir)
            })
          
          # Return a signaled error instead of output
          if (!is.null(cnd)) output <- cnd
          
          # Save the result to file
          saveRDS(output, file.path(p_dir, 'output.rds'))
          file.create(file.path(p_dir, 'done'))
          
        })
      }
      
  })
}




# Runs in a separate R process.
# Cleans up jobqueue's spawned processes when 
# jobqueue's main process (ps_string) terminates.

p__monitor <- function (ppid, mqid, m_dir) {
  
  file_create(m_dir, c('pid_', p__ps_string()))
  
  parent <- p__ps_handle(ppid)
  active <- list()
  
  mq <- interprocess::msg_queue(mqid, 'exists')
  on.exit(mq$remove(), add = TRUE)
  
  
  # Remove all traces of a subprocess
  scrub <- function (pid) {
    
    ps  <- active[[pid]]$ps
    sem <- active[[pid]]$sem
    dir <- active[[pid]]$dir
    
    active[[pid]] <<- NULL
    
    try(silent = TRUE, ps::ps_kill(c(
      ps,
      ps::ps_children(ps, recursive = TRUE),
      ps::ps_find_tree(marker = sem) )))
    
    interprocess::semaphore(sem)$remove()
    try(silent = TRUE, dir_delete(dir))
  }
  
  
  # Check message queue; scrub dead processes
  update_active <- function (scrub_all = FALSE) {
    
    while (!is.null(msg <- mq$receive(0))) {
      
      pid <- substr(msg, 1, 12)
      
      active[[pid]] <<- list(
        ps  = p__ps_handle(pid),
        sem = substr(msg, 14, 24),
        dir = substr(msg, 26, nchar(msg)) )
    }
    
    for (pid in names(active))
      if (!ps::ps_is_running(active[[pid]]$ps) || scrub_all)
        scrub(pid)
  }
  
  
  # Do housekeeping tasks every 2 seconds
  repeat {
    update_active()
    if (!ps::ps_is_running(parent)) { warning('parent ', ppid, ' not running');    break }
    if (ps::ps_wait(parent, 2000))  { warning('ps_wait returned TRUE for ', ppid); break }
  }
  
  
  # Parent session has terminated
  update_active(scrub_all = TRUE)
  
  # Last attempt for wayward processes
  ps::ps_kill(ps::ps_find_tree(marker = mqid))
  
}


p__spawn_monitor <- function () {
  
  if (is.null(ENV$mqid)) {
    
    mq <- interprocess::msg_queue(
      assert    = 'create',
      max_count = 100L,
      max_nchar = 1024L )
    
    ENV$mqid  <- mq$name
    ENV$m_dir <- dir_create(ENV$jq_dir, 'M')
    
    # A script that calls p__monitor.
    cmdfile <- file.path(ENV$m_dir, 'cmds.r')
    writeLines(con = cmdfile, c(
      paste0('ppid  <- ', shQuote(ENV$pid)),
      paste0('mqid  <- ', shQuote(ENV$mqid)),
      paste0('m_dir <- ', shQuote(ENV$m_dir)),
      paste0('jobqueue:::p__monitor(ppid, mqid, m_dir)') ))
    
    # Start the monitor's subprocess.
    system2(
      command = file.path(R.home('bin'), 'Rscript'),
      args    = c('--vanilla', shQuote(cmdfile)),
      wait    = FALSE,
      stdout  = file.path(ENV$m_dir, 'stdout.txt'),
      stderr  = file.path(ENV$m_dir, 'stderr.txt') )
    
    later(p__poll_monitor, delay = 5)
  }
    
  invisible()
}


p__poll_monitor <- function () {
  
  if (is.null(ENV$m_ps))
      ENV$m_ps <- dir_ps(ENV$m_dir)
  
  if (!inherits(ENV$m_ps, 'ps_handle') || !ps::ps_is_running(ENV$m_ps))
    stop(c('subprocess monitor is not running', read_logs(ENV$m_dir))) # nocov
  
  later(p__poll_monitor, delay = 2)
}




# Functionality also available in ps > 1.9.1

p__ps_string <- function(p = ps::ps_handle()) {
  
  pid  <- ps::ps_pid(p)
  time <- as.numeric(ps::ps_create_time(p))
  
  if (.Platform$OS.type == "unix")
    time <- time - as.numeric(ps::ps_boot_time())
  
  time <- round(time, 3) * 1000 # millisecond resolution
  
  map <- c(letters, LETTERS, 0:9)
  paste(
    collapse = '',
    map[
      1 +
        c(
          floor(pid  / 62^(3:0)) %% 62,
          floor(time / 62^(7:0)) %% 62
        )
    ]
  )
}


p__ps_handle <- function(pid = NULL, time = NULL) {
  
  if (!is.character(pid))
    return (ps::ps_handle(pid, time))
  
  str <- pid
  
  map <- structure(0:61, names = c(letters, LETTERS, 0:9))
  val <- map[strsplit(str, '', fixed = TRUE)[[1]]]
  pid <- sum(val[01:04] * 62^(3:0))
  
  tryCatch(
    expr = {
      p <- ps::ps_handle(pid = pid)
      stopifnot(str == p__ps_string(p))
      p
    },
    error = function(e) {
      
      time <- sum(val[05:12] * 62^(7:0)) / 1000
      
      if (.Platform$OS.type == "unix")
        time <- time + as.numeric(ps::ps_boot_time())
      
      time <- as.POSIXct(time, tz = 'GMT', origin = '1970-01-01')
      ps::ps_handle(pid = pid, time = time)
    }
  )
}


