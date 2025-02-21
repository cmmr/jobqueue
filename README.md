
# jobqueue

<!-- badges: start -->
[![cran](https://www.r-pkg.org/badges/version/jobqueue)](https://CRAN.R-project.org/package=jobqueue)
[![conda](https://anaconda.org/conda-forge/r-jobqueue/badges/version.svg)](https://anaconda.org/conda-forge/r-jobqueue)
[![downloads](https://cranlogs.r-pkg.org/badges/grand-total/jobqueue)](https://cranlogs.r-pkg.org/)
[![dev](https://github.com/cmmr/jobqueue/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/cmmr/jobqueue/actions/workflows/R-CMD-check.yaml)
[![covr](https://codecov.io/gh/cmmr/jobqueue/graph/badge.svg)](https://app.codecov.io/gh/cmmr/jobqueue)
<!-- badges: end -->


The goals of jobqueue are to:

  * Run jobs in parallel on background processes.
  * Allow jobs to be stopped at any point.
  * Process job results with asynchronous callbacks.


## Installation

``` r
# Install the latest stable version from CRAN:
install.packages("jobqueue")

# Or the development version from GitHub:
install.packages("pak")
pak::pak("cmmr/jobqueue")
```


## Example

``` r
library(jobqueue)

q <- Queue$new()

job <- q$run({ paste('Hello', 'world!') })
job$result
#> [1] "Hello world!"
```


## Asynchronous Callbacks

Main article: `vignette('hooks')`

``` r
j <- q$run(
  expr  = { 42 }, 
  hooks = list(
    'created' = ~{ message("We're uid '", .$uid, "'.") },
    '*'       = ~{ message('  - ', .$state) }))
#> We're uid 'J2'.
#>   - created
#>   - submitted
#>   - queued
#>   - starting
#>   - running
#>   - done

j$on('done', function (job) message('result = ', job$result))
#> result = 42
```


### Converting to Promises

``` r
job      <- q$run({ 3.14 })
callback <- function (result) message('resolved with: ', result)

job %...>% callback
#> resolved with: 3.14

job %>% then(callback)
#> resolved with: 3.14

as.promise(job)$then(callback)
#> resolved with: 3.14
```

See also https://rstudio.github.io/promises/


### Shiny Integration

```r
function(input, output, session) {
  output$plot <- renderPlot({
    q$run({ read.table(url) }, list(url = input$url)) %...>%
      head(input$n) %...>%
      plot()
  })
}
```

See also https://rstudio.github.io/promises/articles/promises_06_shiny.html


## Stopping Jobs

When a running job is stopped, the background process for it is terminated. 
Terminated background process are automatically replaced by new ones.

Stopped jobs will return a condition object of class 'interrupt' as
their result.

Main article: `vignette('stops')`


#### Manually

``` r
job <- q$run({ Sys.sleep(2); 'Zzzzz' })
job$stop()
job$result
#> <interrupt: job stopped by user>
```

A custom message can also be given, e.g. `job$stop('my reason')`, which
will be returned in the condition object.

#### Runtime Limits

``` r
job <- q$run({ Sys.sleep(2); 'Zzzzz' }, timeout = 0.2)
job$result
#> <interrupt: total runtime exceeded 0.2 seconds>
```

Limits (in seconds) can be set on:

- the total 'submitted' to 'done' time: `timeout = 2`
- on a per-state basis: `timeout = list(queued = 1, running = 2)`
- or both: `timeout = list(total = 3, queued = 2, running = 2)`

#### Stop ID

New jobs will replace existing jobs with the same `stop_id`.

``` r
job1 <- q$run({ Sys.sleep(1); 'A' }, stop_id = 123)
job2 <- q$run({ 'B' },               stop_id = 123)
job1$result
#> <interrupt: duplicated stop_id>
job2$result
#> [1] "B"
```

#### Copy ID

New jobs will mirror the output of existing jobs with the same `copy_id`.

``` r
job1 <- q$run({ Sys.sleep(1); 'A' }, copy_id = 456)
job2 <- q$run({ 'B' },               copy_id = 456)
job1$result
#> [1] "A"
job2$result
#> [1] "A"
```


## Variables

``` r
q    <- Queue$new(globals = list(G = 8))
expr <- quote(c(x = x , y = y, G = G))
job  <- q$run(expr, vars = list(x = 10, y = 2))

dput(job$result)
#> c(x = 10, y = 2, G = 8)
```

