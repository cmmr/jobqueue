
test_that('job', {
  
  skip_on_cran()

  # library(jobqueue); library(testthat)
  
  q <- expect_silent(jobqueue(
    workers = 1L, 
    timeout = c(starting = 15, total = 15) ))
  
  job <- expect_silent( q$run({ TRUE }) )
  expect_identical(job, q$submit(job))
  expect_true( job$result )
  expect_identical(job, q$submit(job))
  
  stop_x <- quote(stop('x'))
  
  job <- expect_silent( q$run(stop_x, signal = FALSE) )
  expect_s3_class( job$result, 'error' )
  
  job <- expect_silent( q$run(stop_x, signal = TRUE) )
  p   <- expect_silent( as.promise(job) %>% then(~{NULL}, ~{NULL}) )
  expect_error( job$result )
  expect_true(  job$signal )
  
  job <- expect_silent( q$run(
    expr     = quote(2 + 3),
    cpus     = NULL,
    timeout  = ~{ list('queued' = 10) },
    reformat = function (j) {
      o <- j$output
      if (!is.numeric(o)) stop(o)
      return (o * 2)
    }) )
  
  expect_true(is.function(job$reformat))
  expect_identical(job$result, 10)

  job <- expect_silent( q$run({ 2 + 3 }) )
  p   <- expect_silent( as.promise(job) %>% then(~{NULL}, ~{NULL}) )

  expect_error( job$proxy <- 'not a Job' )
  expect_error( job$state <- 'done'      )

  expect_identical( job$output, 5                 )
  expect_error(     job$state <- 'not done'       )
  expect_silent(    job$state <- 'done'           )
  expect_no_error(  suppressMessages(job$print()) )
  expect_true(      is.list(job$hooks)            )
  expect_true(      is.list(job$timeout)          )
  expect_true(      startsWith(job$uid, 'J')      )

  job1 <- expect_silent( q$run({ Sys.sleep(100) }, hooks = list('submitted' = class)) )
  job2 <- expect_silent( job_class$new({4}) )
  
  expect_error(     job2$proxy  <- job2        )
  expect_silent(    job2$proxy  <- job1        )
  expect_error(     job2$state  <- 'not proxy' )
  expect_silent(    job1$output <- 'custom'    )
  expect_identical( job1$result , job2$result  )
  expect_silent(    job2$proxy  <- job1        )
  
  j <- expect_silent( q$run({ Sys.sleep(100) }) )
  expect_s3_class( j$stop(), 'job'       )
  expect_s3_class( j$result, 'interrupt' )
  
  expect_silent( q$stop() )
  
})

