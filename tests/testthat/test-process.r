test_that('process', {
  
  skip_on_cran()
  
  
  # library(jobqueue); library(testthat)
  
  mq <- interprocess::msg_queue(max_count = 2, max_nchar = 1024)
  on.exit(mq$remove(), add = TRUE)
  
  w_dir <- dir_create(tempfile())
  on.exit(dir_delete(w_dir), add = TRUE)
  
  
  
  # Code coverage for `p__start()`
  
  ppid   <- p__ps_string()
  mqid   <- mq$name
  p_dir  <- dir_create(w_dir, 'P')
  config <- file.path(w_dir, 'config.rds')
  
  saveRDS(
    file   = config, 
    object = list(
      globals   = list(p_dir = p_dir),
      packages  = 'interprocess',
      namespace = 'jobqueue',
      init      = quote(dir_sem(p_dir)$post())
    ))
  
  saveRDS(
    file   = file.path(p_dir, 'request.rds'), 
    object = list(
      cpus = 1L,
      expr = quote(stop(file.remove(ready_file))),
      vars = list(
        ready_file = file.path(p_dir, 'ready') )
    ))
  
  expect_error(p__start(ppid, mqid, p_dir, config))
  
  
  # Verify correct message to monitor
  expect_identical(mq$count(), 1L)
  msg <- mq$receive(timeout_ms = 0)
  expect_identical(msg, paste(ppid, dir_sem(p_dir), dir_create(tempdir())))
  
  
  
  
  # Code coverage for `p__monitor()`
  ps        <- p__ps_handle()
  real_pid  <- ps::ps_pid(ps)
  real_time <- ps::ps_create_time(ps)
  
  ppid  <- p__ps_string(ps::ps_handle(real_pid, real_time + 5))
  mqid  <- mq$name
  m_dir <- dir_create(w_dir, 'M')
  
  pid   <- p__ps_string(ps::ps_handle(real_pid, real_time + 10))
  sem   <- dir_sem(p_dir)$name
  dir   <- p_dir
  
  mq$send(paste(pid, sem, dir))
  expect_warning(p__monitor(ppid, mqid, m_dir))
  expect_false(dir.exists(p_dir))
  
})
