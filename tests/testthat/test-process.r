test_that('process', {
  
  tmp <- normalizePath(tempfile('ttp'), winslash = '/', mustWork = FALSE)
  
  dir.create(tmp, recursive = TRUE)
  on.exit(unlink(tmp, recursive = TRUE))

  fp <- function (path) file.path(tmp, path)


  # Successful setup and evaluation
  config  <- list(packages = 'base')
  request <- list(expr = quote(TRUE), vars = list(ps_exe = ps::ps_exe), cpus = 1L)
  save_rds(tmp, 'config', 'request')

  res <- expect_silent(p__start(tmp = tmp, testing = TRUE))
  expect_null(res)
  expect_true(readRDS(fp('output.rds')))

  ps_info <- readRDS(fp('ps_info.rds'))
  ps      <- ps::ps_handle()
  expect_identical(ps_info$pid,  ps::ps_pid(ps))
  expect_identical(ps_info$time, ps::ps_create_time(ps))


  # Error during setup
  config <- list(init = quote(stop('test')))
  request <- list(expr = quote(TRUE), vars = list(), cpus = 1L)
  save_rds(tmp, 'config', 'request')

  res <- expect_silent(p__start(tmp = tmp, testing = TRUE))
  expect_null(res)
  cnd <- expect_silent(readRDS(fp('error.rds')))
  expect_s3_class(cnd, 'error')
  expect_identical(cnd$message, 'test')


  # Error during evaluation
  config  <- list(globals = list(x = 'r', f = function () NULL))
  request <- list(expr = quote(stop(x, y)), vars = list(y = 5), cpus = 1L)
  save_rds(tmp, 'config', 'request')
  
  config_fp <- file.path(tmp, 'config.rds')
  alt_fp    <- file.path(tmp, 'alt.rds')
  file.rename(config_fp, alt_fp)
  saveRDS(object = alt_fp, file = config_fp)
  
  res <- expect_silent(p__start(tmp = tmp, testing = TRUE))
  expect_null(res)
  output <- expect_silent(readRDS(fp('output.rds')))
  expect_s3_class(output, 'error')
  expect_identical(output$message, 'r5')

})
