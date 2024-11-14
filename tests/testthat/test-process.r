test_that('process', {
  
  dir.create(wd <- tempfile())
  on.exit(unlink(wd, recursive = TRUE))
  
  fp <- function (path) file.path(wd, path)
  
  semaphore <- create_semaphore(value = 10)
  save_rds(wd, 'semaphore')
  
  
  # Successful setup and evaluation
  config  <- list(packages = 'base')
  request <- list(expr = quote(TRUE), vars = list(), cpus = 1L)
  save_rds(wd, 'config', 'request')
  
  res <- expect_silent(p__start(wd = wd, testing = TRUE))
  expect_null(res)
  expect_true(readRDS(fp('output.rds')))
  
  ps_info <- readRDS(fp('ps_info.rds'))
  ps      <- ps::ps_handle()
  expect_identical(ps_info$pid,  ps::ps_pid(ps))
  expect_identical(ps_info$time, ps::ps_create_time(ps))
  
  
  # Error during setup
  config <- list(init = quote(stop('test')))
  request <- list(expr = quote(TRUE), vars = list(), cpus = 1L)
  save_rds(wd, 'config', 'request')
  
  res <- expect_silent(p__start(wd = wd, testing = TRUE))
  expect_null(res)
  cnd <- expect_silent(readRDS(fp('error.rds')))
  expect_s3_class(cnd, 'error')
  expect_identical(cnd$message, 'test')
  
  
  # Error during evaluation
  config <- list(globals = list(x = 'r'))
  request <- list(expr = quote(stop(x, y)), vars = list(y = 5), cpus = 1L)
  save_rds(wd, 'config', 'request')
  
  res <- expect_silent(p__start(wd = wd, testing = TRUE))
  expect_null(res)
  output <- expect_silent(readRDS(fp('output.rds')))
  expect_s3_class(output, 'error')
  expect_identical(output$message, 'r5')
  
  
  remove_semaphore(semaphore)
})