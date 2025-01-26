test_that('process', {
  
  tmp_dir <- normalizePath(tempfile('ttp'), winslash = '/', mustWork = FALSE)

  semaphore <- create_semaphore(value = 10)
  sem_dir   <- file.path(tmp_dir, semaphore)
  
  dir.create(sem_dir, recursive = TRUE)
  on.exit(unlink(tmp_dir, recursive = TRUE))

  fp <- function (path) file.path(sem_dir, path)


  # Successful setup and evaluation
  config  <- list(packages = 'base')
  request <- list(expr = quote(TRUE), vars = list(ps_exe = ps::ps_exe), cpus = 1L)
  save_rds(sem_dir, 'config', 'request')

  res <- expect_silent(p__start(sem_dir = sem_dir, testing = TRUE))
  expect_null(res)
  expect_true(readRDS(fp('output.rds')))

  ps_info <- readRDS(fp('ps_info.rds'))
  ps      <- ps::ps_handle()
  expect_identical(ps_info$pid,  ps::ps_pid(ps))
  expect_identical(ps_info$time, ps::ps_create_time(ps))


  # Error during setup
  config <- list(init = quote(stop('test')))
  request <- list(expr = quote(TRUE), vars = list(), cpus = 1L)
  save_rds(sem_dir, 'config', 'request')

  res <- expect_silent(p__start(sem_dir = sem_dir, testing = TRUE))
  expect_null(res)
  cnd <- expect_silent(readRDS(fp('error.rds')))
  expect_s3_class(cnd, 'error')
  expect_identical(cnd$message, 'test')


  # Error during evaluation
  config <- list(globals = list(x = 'r'))
  request <- list(expr = quote(stop(x, y)), vars = list(y = 5), cpus = 1L)
  save_rds(sem_dir, 'config', 'request')

  res <- expect_silent(p__start(sem_dir = sem_dir, testing = TRUE))
  expect_null(res)
  output <- expect_silent(readRDS(fp('output.rds')))
  expect_s3_class(output, 'error')
  expect_identical(output$message, 'r5')


  remove_semaphore(semaphore)
  
  rm(list = setdiff('tmp_dir', ls()))
})
