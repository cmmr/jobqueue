
test_that('worker', {
  
  # library(jobqueue); library(testthat)
  w <- expect_silent(Worker$new(wait = FALSE))
  expect_error(w$run(Job$new({ 1 })))

  expect_silent(w$on('starting', ~{ NULL }))
  expect_silent(w$on('*',        ~{ NULL }))

  expect_error(w$start())
  expect_no_error(suppressMessages(w$print()))
  expect_true(is.list(w$hooks))
  expect_true(is.null(w$job))
  expect_true(dir.exists(w$tmp))
  expect_true(startsWith(w$uid, 'W'))

  expect_silent(w$wait(timeout = 10))
  expect_true(inherits(w$ps, 'ps_handle'))
  expect_error(w$run('not a Job'))

  expect_silent(w$run(Job$new({ stop() })))
  expect_silent(w$wait(timeout = 10))
  
  expect_silent(w$run(Job$new({ q() })))
  expect_silent(w$wait(timeout = 10))
  
  expect_silent(w$run(Job$new({ NULL })))
  expect_silent(w$wait(timeout = 10))

  expect_silent(w$stop())
  expect_error(w$run(Job$new({ 1 })))
  
  expect_error( Worker$new(init = { stop() }, wait = 10))
  expect_error( Worker$new(init = { q()    }, wait = 10))
  
  w1 <- expect_silent(Worker$new(init = { stop() }, wait = FALSE))
  w2 <- expect_silent(Worker$new(init = { q()    }, wait = FALSE))
  w1$wait(timeout = 10)
  w2$wait(timeout = 10)
  
  expect_s3_class(w1$reason, 'error')
  expect_s3_class(w2$reason, 'error')
  
})

