
test_that('worker', {
  
  skip_on_cran()
  
  # library(jobqueue); library(testthat)
  
  w <- expect_silent(worker_class$new())
  expect_silent(w$restart())
  expect_silent(w$stop())
  
  w <- expect_silent(worker_class$new(wait = FALSE))
  expect_silent(w$wait('.next', timeout = 15))

  expect_silent(w$on('busy', ~{ NULL }))
  expect_silent(w$on('*',    ~{ NULL }))

  expect_error(w$start(), 'worker is not stopped', 'error')
  expect_no_error(suppressMessages(w$print()))
  expect_true(is.list(w$hooks))
  expect_true(is.null(w$job))
  expect_true(is.null(w$cnd))
  expect_true(dir.exists(w$tmp))
  expect_true(startsWith(w$uid, 'W'))

  expect_true(inherits(w$ps, 'ps_handle'))
  expect_error(w$run('not a job'), 'not a job', 'error')

  expect_silent(w$run(job_class$new({ stop() })))
  expect_silent(w$wait(timeout = 15))

  expect_silent(w$run(job_class$new({ NULL })))
  expect_silent(w$wait(timeout = 15))
  
  j <- job_class$new({ Sys.sleep(100) }, signal = TRUE)
  expect_silent(w$run(j))
  expect_in(ps_kill(w$ps), c('killed', 'terminated'))
  expect_error(j$result, 'worker subprocess terminated unexpectedly', 'error')
  expect_silent(w$wait(timeout = 100))
  expect_identical(w$state, 'idle')

  expect_silent(w$stop('terminated'))
  expect_error(w$wait('.next', timeout = 15))
  expect_error(w$run(job_class$new({ 1 })), 'terminated', 'error')
  
  
  skip_on_covr()

  expect_error(worker_class$new(init = { stop() }, timeout = 15))
  w <- expect_silent(worker_class$new(init = { stop() }, wait = FALSE))
  expect_error(w$wait(timeout = 15))

  w <- expect_silent(worker_class$new(wait = FALSE))
  expect_silent(w$run(job_class$new({ q('no') })))
  expect_silent(w$wait(timeout = 15))
  expect_silent(w$stop())

  expect_error(worker_class$new(init = { q('no') }, timeout = 15))

  w <- expect_silent(worker_class$new(init = { q('no') }, wait = FALSE))
  expect_error(w$wait(timeout = 15))
  
})

