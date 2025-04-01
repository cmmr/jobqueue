test_that('process', {
  
  # library(testthat)
  
  tmp <- normalizePath(tempfile('ttp'), winslash = '/', mustWork = FALSE)
  
  dir.create(tmp, recursive = TRUE)
  on.exit(unlink(tmp, recursive = TRUE))

  fp <- function (path) file.path(tmp, path)


  # Successful setup and evaluation
  config  <- list(packages = 'base', namespace = 'R6')
  request <- list(expr = quote(TRUE), vars = list(ps_exe = ps::ps_exe), cpus = 1L)
  save_rds(tmp, 'config', 'request')

  res <- expect_silent(p__start(tmp = tmp, testing = TRUE))
  expect_true(res)
  expect_true(readRDS(fp('output.rds')))

  ps_info <- readRDS(fp('ps_info.rds'))
  ps      <- ps::ps_handle()
  expect_identical(ps_info$pid,  ps::ps_pid(ps))
  expect_identical(ps_info$time, ps::ps_create_time(ps))


  # Error during setup
  config  <- list(init = quote(stop('test', call. = FALSE)))
  request <- list(expr = quote(TRUE), vars = list(), cpus = 1L)
  save_rds(tmp, 'config', 'request')

  res <- expect_silent(p__start(tmp = tmp, testing = TRUE))
  expect_s3_class(res, 'error')
  expect_identical(res$message, 'test')
  expect_identical(res, readRDS(fp('error.rds')))
  
  
  # Package doesn't exist
  config  <- list(packages = 'Q4Er6o9iAW')
  request <- list(expr = quote(TRUE), vars = list(), cpus = 1L)
  save_rds(tmp, 'config', 'request')
  
  res <- expect_silent(p__start(tmp = tmp, testing = TRUE))
  expect_s3_class(res, 'error')
  expect_identical(res$message, 'Could not load package: Q4Er6o9iAW')
  expect_identical(res, readRDS(fp('error.rds')))
  
  
  # Package isolation
  config  <- list(packages = 'ps')
  request <- list(expr = quote(exists('ps_pid')), vars = list(), cpus = 1L)
  testing <- function (tmp) { if (exists('ps_pid')) stop('contaminated', call. = FALSE) }
  save_rds(tmp, 'config', 'request')
  
  res <- expect_silent(p__start(tmp = tmp, testing = testing))
  expect_true(res)
  expect_true(readRDS(fp('output.rds')))
  expect_null(readRDS(fp('error.rds')))
  
  
  # Abandoned child
  config  <- list()
  request <- list(expr = quote(TRUE), vars = list(), cpus = 1L)
  testing <- function (tmp) { unlink(file.path(tmp, '_ready_')) }
  save_rds(tmp, 'config', 'request')
  
  res <- expect_silent(p__start(tmp = tmp, testing = testing))
  expect_s3_class(res, 'error')
  expect_identical(res$message, 'ready file missing')
  expect_identical(res, readRDS(fp('error.rds')))


  # Error during evaluation
  config  <- list(globals = list(x = 'r', f = function () NULL))
  request <- list(expr = quote(stop(x, y)), vars = list(y = 5), cpus = 1L)
  save_rds(tmp, 'config', 'request')
  
  config_fp <- file.path(tmp, 'config.rds')
  alt_fp    <- file.path(tmp, 'alt.rds')
  invisible(file.rename(config_fp, alt_fp))
  saveRDS(object = alt_fp, file = config_fp)
  
  res <- expect_silent(p__start(tmp = tmp, testing = TRUE))
  expect_s3_class(res, 'error')
  expect_identical(res$message, 'r5')
  expect_identical(res, readRDS(fp('output.rds')))
  
  
})
