
test_that('basic', {
  
  # library(jobqueue); library(testthat)
  q <- expect_silent(Queue$new(workers = 1L, timeout = 5.2))
  expect_equal(q$state, 'starting')

  expect_no_error(q$tmp)
  expect_no_error(suppressMessages(q$print()))

  job <- expect_silent(q$run({ 2 + 2 }))
  expect_false(job$is_done)
  expect_equal(job$result, 4)
  expect_equal(job$state, 'done')
  expect_true(job$is_done)

  job <- expect_silent(q$run({ x + y }, vars = list(x = 3, y = 5)))
  expect_equal(job$result, 8)

  expect_equal(q$state, 'idle')
  expect_equal(length(q$workers), 1L)

  expect_true(is.list(q$hooks))
  expect_true(startsWith(q$uid, 'Q'))

  expect_error(q$submit('not a Job'))
  
  expect_silent(q$stop())

  expect_equal(q$state, 'stopped')
  expect_equal(length(q$workers), 0)
  
})




test_that('config', {

  e <- new.env(parent = emptyenv())

  q <- expect_silent(Queue$new(
    workers  = 1L,
    timeout = 5.3,
    globals  = list(x = 42),
    packages = 'magrittr',
    init     = { y <- 37 },
    hooks    = list(
      'q_idle'  = ~{ e$state = .$state },
      'q_.next' = ~{ e$.next = .$state },
      'q_*'     = ~{ e$.star = .$state }
    )))

  q$on('stopped', function () { e$state = 'stopped' })
  q$on('stopped', class) # A primitive; for code coverage.

  job <- q$run({ c(x, y) %>% sum })

  expect_equal(job$result, 42 + 37)
  expect_equal(e$state, 'idle')
  expect_equal(e$.next, 'starting')
  expect_equal(e$.star, 'idle')

  expect_silent(q$stop())
  expect_equal(e$state, 'stopped')
  expect_equal(e$.next, 'starting')
  expect_equal(e$.star, 'stopped')
  
})


test_that('workers', {

  q <- expect_silent(Queue$new(workers = 2L, max_cpus = 3L, timeout = 5.4))

  q$run({ Sys.sleep(1) })
  q$run({ Sys.sleep(1) })
  q$run({ Sys.sleep(1) })

  expect_equal(map(q$jobs, 'state'), rep('queued', 3))

  q$wait('.next')
  expect_equal(map(q$jobs, 'state'), c('running', 'running', 'queued'))
  expect_equal(map(q$workers, 'state'), rep('busy', 2))

  expect_silent(q$stop())
  
})


test_that('max_cpus', {

  q <- expect_silent(Queue$new(workers = 3L, max_cpus = 2L, timeout = 5.5))

  q$run({ Sys.sleep(1) })
  q$run({ Sys.sleep(1) })
  q$run({ Sys.sleep(1) })

  expect_equal(map(q$jobs, 'state'), rep('queued', 3))

  q$wait('.next')
  q$run({ Sys.sleep(1) })

  expect_equal(map(q$jobs, 'state'), c(rep('running', 2), rep('queued', 2)))
  expect_equal(map(q$workers, 'state'), c('busy', 'busy', 'idle'))

  expect_silent(q$stop())
  
})


test_that('interrupt', {

  q <- expect_silent(Queue$new(workers = 1L, timeout = 5.6))

  job <- q$run({ Sys.sleep(1) })
  expect_silent(job$stop())
  expect_s3_class(job$result, class = c('interrupt', 'condition'))

  job <- q$run({ Sys.sleep(1) }, timeout = 0.1)
  expect_s3_class(job$result, class = c('interrupt', 'condition'))

  job1 <- q$run({ Sys.sleep(1); 'A' }, stop_id = function (job) 123)
  job2 <- q$run({ 'B' },               stop_id = ~{ 123 })

  expect_s3_class(job1$result, class = c('interrupt', 'condition'))
  expect_equal(job2$result, 'B')

  job1 <- q$run({ Sys.sleep(0.1); 'A' }, copy_id = ~{ 456 })
  job2 <- q$run({ 'B' },                 copy_id = 456)
  expect_equal(job1$result, 'A')
  expect_equal(job2$result, 'A')

  expect_silent(q$stop())
  
})

