
test_that('job', {

  # library(jobqueue); library(testthat)
  q <- expect_silent( Queue$new(workers = 1L) )
  
  job <- expect_silent( q$run({ TRUE }) )
  expect_true( job$result )
  
  job <- expect_silent( q$run({ stop('x') }, signal = FALSE) )
  expect_s3_class( job$result, 'error' )
  
  job <- expect_silent( q$run({ stop('x') }, signal = TRUE) )
  p   <- expect_silent( as.promise(job)  )
  expect_error( job$result )
  expect_true(  job$signal )
  
  
  job <- expect_silent( q$run(
    expr     = quote(2 + 3),
    cpus     = NULL,
    timeout  = list('queued' = 10),
    reformat = function (j) {
      o <- j$output
      if (!is.numeric(o)) stop(o)
      return (o * 2)
    }) )

  expect_true(is.function(job$reformat))
  expect_identical(job$result, 10)

  job <- expect_silent( q$run({ 2 + 3 }) )
  p   <- expect_silent( as.promise(job)  )

  expect_error( job$proxy <- 'not a Job' )
  expect_error( job$state <- 'done'      )

  expect_identical(job$output, 5)
  expect_error(    job$state <- 'not done'       )
  expect_silent(   job$state <- 'done'           )
  expect_no_error( suppressMessages(job$print()) )
  expect_true(     is.list(job$hooks)            )
  expect_true(     is.list(job$timeout)          )
  expect_true(     startsWith(job$uid, 'J')      )

  job1 <- expect_silent( q$run({ Sys.sleep(1) }, hooks = list('submitted' = class)) )
  job2 <- expect_silent( Job$new({4}) )

  expect_silent(    job2$proxy  <- job1        )
  expect_error(     job2$state  <- 'not proxy' )
  expect_silent(    job1$output <- 'custom'    )
  expect_identical( job1$result , job2$result  )
  
  expect_silent( q$stop() )
})

