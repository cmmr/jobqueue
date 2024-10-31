
test_that('job', {

  q <- expect_silent(Queue$new(workers = 1L))
  
  job <- expect_silent(q$run(
    expr     = quote(2 + 3), 
    cpus     = NULL,
    lazy     = TRUE, 
    timeout  = list('queued' = 1),
    reformat = ~{ .$output * 2 }))
  
  expect_true(is.function(job$reformat))
  expect_equal(job$result, 10)
  p <- expect_silent(as.promise(job))
  
  job <- expect_silent(q$run({ 2 + 3 }, reformat = NULL))
  p   <- expect_silent(as.promise(job))
  expect_error(job$proxy <- 'not a Job')
  expect_error(job$state <- 'done')
  
  expect_equal(job$output, 5)
  expect_error(job$state <- 'not done')
  expect_silent(job$state <- 'done')
  
  expect_true(is.list(job$hooks))
  expect_true(is.list(job$timeout))
  expect_true(startsWith(job$uid, 'J'))
  expect_no_error(suppressMessages(job$print()))
  
  
  job1 <- expect_silent(q$run({ Sys.sleep(1) }, hooks = list('submitted' = class)))
  job2 <- expect_silent(Job$new({4}))
  expect_silent(job2$proxy <- job1)
  expect_error(job2$state <- 'not proxy')
  expect_silent(job1$output <- "custom")
  expect_equal(job1$result, job2$result)
  

  expect_silent(q$shutdown())
})
