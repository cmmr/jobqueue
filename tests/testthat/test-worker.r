
test_that('worker', {
  
  skip_on_cran()
  
  # library(jobqueue); library(testthat)
  
  w <- expect_silent(Worker$new())
  expect_silent(w$restart())
  expect_silent(w$stop())
  
  w <- expect_silent(Worker$new(wait = FALSE))
  expect_silent(w$wait('.next', timeout = 15))

  expect_silent(w$on('busy', ~{ NULL }))
  expect_silent(w$on('*',    ~{ NULL }))

  expect_error(w$start(), 'Worker is not stopped', 'error')
  expect_no_error(suppressMessages(w$print()))
  expect_true(is.list(w$hooks))
  expect_true(is.null(w$job))
  expect_true(is.null(w$cnd))
  expect_true(dir.exists(w$tmp))
  expect_true(startsWith(w$uid, 'W'))

  expect_true(inherits(w$ps, 'ps_handle'))
  expect_error(w$run('not a Job'), 'not a Job', 'error')

  expect_silent(w$run(Job$new({ stop() })))
  expect_silent(w$wait(timeout = 15))

  expect_silent(w$run(Job$new({ NULL })))
  expect_silent(w$wait(timeout = 15))
  
  j <- Job$new({ Sys.sleep(100) }, signal = TRUE)
  expect_silent(w$run(j))
  expect_in(ps_kill(w$ps), c('killed', 'terminated'))
  expect_error(j$result, 'worker subprocess terminated unexpectedly', 'error')
  expect_silent(w$wait(timeout = 100))
  expect_identical(w$state, 'idle')

  expect_silent(w$stop('terminated'))
  expect_error(w$wait('.next', timeout = 15))
  expect_error(w$run(Job$new({ 1 })), 'terminated', 'error')
  
  
  skip_on_covr()

  expect_error(Worker$new(init = { stop() }, timeout = 15))
  w <- expect_silent(Worker$new(init = { stop() }, wait = FALSE))
  expect_error(w$wait(timeout = 15))

  w <- expect_silent(Worker$new(wait = FALSE))
  expect_silent(w$run(Job$new({ q('no') })))
  expect_silent(w$wait(timeout = 15))
  expect_silent(w$stop())

  expect_error(Worker$new(init = { q('no') }, timeout = 15))

  w <- expect_silent(Worker$new(init = { q('no') }, wait = FALSE))
  expect_error(w$wait(timeout = 15))
  
})

