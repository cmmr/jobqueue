
test_that('basic', {

  q <- expect_silent(Queue$new(workers = 1L))
  expect_equal(q$state, 'starting')

  expect_no_error(q$print())

  job <- expect_silent(q$run({ 2 + 2 }, scan = FALSE))
  expect_false(job$is_done)
  expect_equal(job$result, 4)
  expect_equal(job$state, 'done')
  expect_true(job$is_done)

  job <- expect_silent(q$run({ x + y }, vars = list(x = 3, y = 5)))
  expect_equal(job$result, 8)

  z   <- 2
  job <- expect_silent(q$run({ x + z }, vars = list(x = 3)))
  expect_equal(job$result, 5)

  expect_equal(q$state, 'active')
  expect_equal(length(q$workers), 1L)

  expect_true(is.list(q$hooks))
  expect_true(startsWith(q$uid, 'Q'))
  expect_setequal(names(q$loaded), c('globals', 'attached'))

  expect_error(q$submit('not a Job'))

  expect_silent(q$shutdown())

  expect_equal(q$state, 'stopped')
  expect_equal(length(q$workers), 0)
})




test_that('config', {

  e <- new.env(parent = emptyenv())

  q <- expect_silent(Queue$new(
    workers  = 1L,
    globals  = list(x = 42),
    packages = 'magrittr',
    init     = { y <- 37 },
    hooks    = list(
      'q_active'  = ~{ e$state = .$state },
      'q_.next'   = ~{ e$.next = .$state },
      'q_*'       = ~{ e$.star = .$state }
    )))

  q$on('stopped', function () { e$state = 'stopped' })
  q$on('stopped', class) # A primitive; for code coverage.

  x <- y <- 1
  job <- q$run({c(x, y) %>% sum})

  expect_equal(job$result, 42 + 37)
  expect_equal(e$state, 'active')
  expect_equal(e$.next, 'active')
  expect_equal(e$.star, 'active')

  expect_silent(q$shutdown())
  expect_equal(e$state, 'stopped')
  expect_equal(e$.next, 'active')
  expect_equal(e$.star, 'stopped')
})


test_that('workers', {

  q <- expect_silent(Queue$new(workers = 2L, max_cpus = 3L))

  q$run({ Sys.sleep(1) }, scan = FALSE)
  q$run({ Sys.sleep(1) }, scan = FALSE)
  q$run({ Sys.sleep(1) }, scan = FALSE)

  expect_equal(map(q$jobs, 'state'), rep('queued', 3))

  q$wait()
  expect_equal(map(q$jobs, 'state'), c('running', 'running', 'queued'))
  expect_equal(map(q$workers, 'state'), rep('busy', 2))

  expect_silent(q$shutdown())
})


test_that('max_cpus', {

  q <- expect_silent(Queue$new(workers = 3L, max_cpus = 2L))

  q$run({ Sys.sleep(1) }, scan = FALSE)
  q$run({ Sys.sleep(1) }, scan = FALSE)
  q$run({ Sys.sleep(1) }, scan = FALSE)

  expect_equal(map(q$jobs, 'state'), rep('queued', 3))

  q$wait()
  q$run({ Sys.sleep(1) }, scan = FALSE)
  
  expect_equal(map(q$jobs, 'state'), c(rep('running', 2), rep('queued', 2)))
  expect_equal(map(q$workers, 'state'), c('busy', 'busy', 'idle'))

  expect_silent(q$shutdown())
})


test_that('interrupt', {

  q <- expect_silent(Queue$new(workers = 1L, scan = FALSE))

  job <- q$run({ Sys.sleep(1) })
  expect_silent(job$stop())
  expect_s3_class(job$result, class = c('interrupt', 'error', 'condition'))

  job <- q$run({ Sys.sleep(1) }, timeout = 0.1)
  expect_s3_class(job$result, class = c('interrupt', 'error', 'condition'))

  job1 <- q$run({ Sys.sleep(1); 'A' }, stop_id = function (job) 123)
  job2 <- q$run({ 'B' },               stop_id = ~{ 123 })

  expect_s3_class(job1$result, class = c('interrupt', 'error', 'condition'))
  expect_equal(job2$result, 'B')

  job1 <- q$run({ Sys.sleep(0.1); 'A' }, copy_id = ~{ 456 })
  job2 <- q$run({ 'B' },                 copy_id = 456)
  expect_equal(job1$result, 'A')
  expect_equal(job2$result, 'A')

  expect_silent(q$shutdown())
})

