
test_that('validate', {
  
  expect_s3_class(validate_list(             ~{ NULL }, func_ok = TRUE), 'function')
  expect_s3_class(validate_hooks(            ~{ NULL }, func_ok = TRUE), 'function')
  expect_s3_class(validate_timeout(          ~{ NULL }, func_ok = TRUE), 'function')
  expect_s3_class(validate_positive_number(  ~{ NULL }, func_ok = TRUE), 'function')
  expect_s3_class(validate_positive_integer( ~{ NULL }, func_ok = TRUE), 'function')
  expect_s3_class(validate_character_vector( ~{ NULL }, func_ok = TRUE), 'function')
  
  expect_identical(validate_string(errorCondition('err'), cnd_ok = TRUE), 'err')
  expect_true(validate_function(TRUE, bool_ok = TRUE))
  
  expect_error(validate_function('not a function'))
  expect_error(validate_function(x ~ y))
  expect_error(validate_expression('not an expression', 'or call'))
  expect_error(validate_list(list('not named'), named = TRUE))
  expect_error(validate_list(list(is = 'named'), named = FALSE))
  expect_error(validate_list(list(not = 'numeric'), of_type = 'numeric'))
  expect_error(validate_timeout(-1))
  expect_error(validate_timeout(list('dup' = "name", 'dup' = "name")))
  expect_error(validate_timeout(list('queued' = 1:5)))
  expect_error(validate_timeout(list('queued' = 'not a number')))
  expect_error(validate_timeout(list('queued' = numeric(0))))
  expect_error(validate_timeout(list('queued' = NA_integer_)))
  expect_error(validate_positive_number('not a number'))
  expect_error(validate_positive_integer('not an integer'))
  expect_error(validate_character_vector(list('not a character vector')))
  expect_error(validate_string(list('not a string')))
  
})

