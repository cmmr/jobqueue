# 
# # Suggests:
# #   httpuv,
# #   webutils,
# 
# # Using `hook` to alter the handler
# # hook <- function (job, queue) {
# #   job$vars$my_str <- function (x) capture.output(ls.str(x[order(names(x))]))
# #   job$expr <- switch(
# #     EXPR = job$vars$req$PATH_INFO, 
# #     '/hello'   = quote('Hello World'), 
# #     '/date'    = quote(date()), 
# #     '/sleep'   = quote({x <- date(); Sys.sleep(5); c(x, date())}),
# #     '/req'     = quote(my_str(req)),
# #     '/headers' = quote(my_str(as.list(req$HEADERS))),
# #     '/query'   = quote(my_str(webutils::parse_query(req$QUERY_STRING))),
# #     job$expr )
# # }
# 
# 
# web_queue <- function (
#     handler,
#     host    = '0.0.0.0', 
#     port    = 8080L, 
#     parse   = TRUE,
#     hook    = NULL,
#     timeout = NULL,
#     stop_id = NULL,
#     copy_id = NULL,
#     globals = NULL, 
#     packages    = NULL, 
#     init    = NULL, 
#     workers = parallelly::availableCores(),
#     standby = ceiling(workers / 4),
#     options = callr::r_session_options(),
#     quiet   = FALSE, 
#     onHeaders         = NULL, 
#     staticPaths       = NULL, 
#     staticPathOptions = NULL ) {
#   
#   WebQueue$new(
#     handler = handler,
#     host    = host,
#     port    = port,
#     parse   = parse,
#     hook    = hook,
#     timeout = timeout,
#     stop_id = stop_id,
#     copy_id = copy_id,
#     globals = globals,
#     packages    = packages,
#     init    = init,
#     workers = workers,
#     standby = standby,
#     options = options,
#     quiet   = quiet, 
#     onHeaders         = onHeaders, 
#     staticPaths       = staticPaths, 
#     staticPathOptions = staticPathOptions )
# }
# 
# 
# 
# WebQueue <- R6Class(
#   classname = "WebQueue",
#   cloneable = FALSE,
#   
#   public = list(
#     
#     initialize = function (
#         handler,
#         host     = '0.0.0.0', 
#         port     = 8080L, 
#         parse    = TRUE,
#         hook     = NULL,
#         timeout  = NULL,
#         stop_id  = NULL,
#         copy_id  = NULL,
#         globals  = NULL, 
#         packages = NULL, 
#         init     = NULL, 
#         workers  = parallelly::availableCores(),
#         standby  = ceiling(workers / 4),
#         options  = callr::r_session_options(),
#         quiet    = FALSE, 
#         onHeaders         = NULL, 
#         staticPaths       = NULL, 
#         staticPathOptions = NULL ) {
#       
#       wq_initialize(
#         self, private, 
#         handler, host, port, parse, 
#         hook, timeout, stop_id, copy_id, 
#         globals, packages, init, workers, standby, options,
#         quiet, onHeaders, staticPaths, staticPathOptions )
#     },
#     
#     print    = function (...) wq_print(self),
#     stop_all = function ()    private$.job_queue$stop_all(),
#     shutdown = function ()    private$finalize()
#   ),
#   
#   private = list(
#     
#     .job_queue = NULL,
#     .server    = NULL,
#     parse      = TRUE,
#     
#     app_call   = function (req) wq__app_call(private, req),
#     
#     finalize   = function () {
#       private$.job_queue$shutdown()
#       private$.server$stop()
#       return (invisible(NULL))
#     }
#   ),
#   
#   active = list(
#     job_queue   = function () private$.job_queue,
#     server      = function () private$.server,
#     pool        = function () private$.job_queue$pool,
#     workers     = function () private$.job_queue$workers,
#     jobs        = function () private$.job_queue$jobs,
#     is_running  = function () private$.server$isRunning(),
#     host        = function () private$.server$getHost(),
#     port        = function () private$.server$getPort()
#   )
# )
# 
# 
# wq_initialize <- function (
#     self, private, 
#     handler, host, port, parse, 
#     hook, timeout, stop_id, copy_id, 
#     globals, packages, init, workers, standby, options,
#     quiet, onHeaders, staticPaths, staticPathOptions ) {
#   
#   if (!nzchar(system.file(package = 'httpuv')))
#     cli_abort("Please install the {.pkg httpuv} R Package from CRAN.")
#   if (!nzchar(system.file(package = 'webutils')))
#     cli_abort("Please install the {.pkg webutils} R Package from CRAN.")
#   
#   # Start a Queue.
#   private$.job_queue <- job_queue(
#     hook     = hook, 
#     expr     = handler, 
#     timeout  = timeout, 
#     stop_id  = stop_id, 
#     copy_id  = copy_id, 
#     globals  = globals, 
#     packages = packages, 
#     init     = init, 
#     workers  = workers, 
#     standby  = standby,
#     options  = options )
#   
#   # Start a 'httpuv' http server.
#   private$.server <- httpuv::startServer(
#     host  = host, 
#     port  = port, 
#     quiet = quiet, 
#     app   = list(
#       call              = private$app_call,
#       onHeaders         = onHeaders, 
#       staticPaths       = staticPaths, 
#       staticPathOptions = staticPathOptions ))
#   
#   # Options for pre-parsing the request.
#   if (is_true(parse)) {
#     
#     private$parse <- function (req) {
#       
#       if (identical(req$REQUEST_METHOD, 'GET')) {
#         if (is_scalar_character(req$QUERY_STRING))
#           req$ARGS <- webutils::parse_query(req$QUERY_STRING)
#         
#       } else if (identical(req$REQUEST_METHOD, 'POST')) {
#         if (is_scalar_character(req$CONTENT_TYPE))
#           req$ARGS <- webutils::parse_http(req$rook.input$read(), req$CONTENT_TYPE)
#       }
#       
#     }
#     
#   } else if (is_false(parse)) {
#     
#     # Streams don't transfer between processes, so slurp it here.
#     private$parse <- function (req) {
#       if (is_function(req$rook.input$read))
#         req$rook.data <- req$rook.input$read()
#     }
#     
#   } else {
#     private$parse <- validate_function(parse)
#   }
#   
#   return (self)
# }
# 
# 
# wq_print <- function (self) {
#   host <- self$host
#   if (host == '0.0.0.0') host <- 'localhost'
#   url <- paste0('http://', host, ':', self$port)
#   cli_text('{.cls {class(self)}} on {.url {url}}')
# }
# 
# 
# wq__app_call <- function (private, req) {
#   
#   if (is_function(private$parse))
#     private$parse(req)
#   
#   job <- q_run(vars = list(req = req))
#   
#   then(
#     promise     = as.promise(job),
#     onFulfilled = wq__format_200,
#     onRejected  = wq__format_500 )
# }
# 
# 
# wq__format_200 <- function (resp) {
#   if (length(resp) == 0)          return (list(status = 200L))
#   if (is_list(resp))              return (resp)
#   if (is_scalar_integerish(resp)) return (list(status = as.integer(resp)))
#   list(status = 200L, body = paste(collapse='\n', as.character(resp)))
# }
# 
# wq__format_500 <- function (resp) {
#   if (is_scalar_integerish(resp)) return (list(status = as.integer(resp)))
#   list(status = 500L, body = paste(collapse='\n', as.character(resp)))
# }
# 
# 
# 
# 
# web_demo <- function () {
#   
#   # A functions that will be available on the worker processes.
#   globals <- list(
#     
#     template = function (header, text) {
#       paste0('
#         <!doctype html>
#         <html lang="en">
#           <head>
#             <meta charset="utf-8">
#             <meta name="viewport" content="width=device-width, initial-scale=1">
#             <meta name="color-scheme" content="light dark">
#             <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@picocss/pico@2/css/pico.min.css">
#             <title>JobQueue | WebQueue Demo</title>
#           </head>
#           <body>
#             <header class="container">
#               <nav>
#                 <ul>
#                   <li><strong>WebQueue Demo</strong></li>
#                 </ul>
#                 <ul>
#                   <li><a href="date">Date</a></li>
#                   <li><a href="greet">Greet</a></li>
#                   <li><a href="req">Request</a></li>
#                   <li><a href="sleep">Sleep</a></li>
#                 </ul>
#               </nav>
#             </header>
#             <main class="container">
#               <h2>', header, '</h2>
#               ', text, '
#             </main>
#             <footer class="container">
#               <small>
#                 See also <a href="https://cmmr.github.io/jobqueue/" class="secondary">https://cmmr.github.io/jobqueue/</a>
#               </small>
#             </footer>
#           </body>
#         </html>
#       ')
#     }, # /template
#     
#     list2str = function (x) {
#       x <- as.list(x)
#       x <- capture.output(ls.str(x[order(names(x))]))
#       paste(collapse = '\n', x)
#     },
#     
#     form = function (name, type, value, button, action = NULL) {
#       value  <- shQuote(ifelse(is.null(value), '', value))
#       method <- ifelse(is.null(action), '', ' method = "POST"')
#       action <- ifelse(is.null(action), '', paste0(' action = "', action, '"'))
#       paste0('
#         <form', method, action, '>
#           <fieldset role="group">
#           <input name="', name, '" type="', type, '" value=', value, '>
#           <input type="submit" value="', button, '">
#           </fieldset>
#         </form> ')
#     }
#   )
#   
#   
#   # Code that gets run on the worker process.
#   handler <- quote({
#     
#     page <- req$PATH_INFO
#     
#     if (page == '/date') {
#       header <- "Curent Date and Time"
#       text   <- paste0('<pre><code>', Sys.time(), '</code></pre>')
#       
#     } else if (page == '/greet') {
#       header <- "Interactive Greeting"
#       text   <- paste0(
#         ifelse(
#           test = is.null(req$ARGS$name), 
#           yes  = "Enter your name to be greeted.", 
#           no   = paste0('<article>Hello, <b>', req$ARGS$name, '</b>!</article>\n') ),
#         '<blockquote>',
#         '<table>',
#         '<tr>',
#         '<td><h4>Using GET:</h4></td>',
#         '<td>', form('name', 'text', req$ARGS$name, 'Greet Me!'), '</td>',
#         '</tr><tr>',
#         '<td><h4>Using POST:</h4></td>',
#         '<td>', form('name', 'text', req$ARGS$name, 'Greet Me!', '/greet'), '</td>',
#         '</tr>',
#         '</table>',
#         '</blockquote>'
#       )
#       
#     } else if (page == '/req') {
#       
#       escape <- function (nm) {
#         val <- get(nm, req)
#         pre <- TRUE
#         
#         if (is.character(val) && length(val) > 1) {
#           val <- mapply(paste, names(val), '=', unname(val))
#           val <- paste(collapse = '\n', val)
#         } else if (!is.character(val) || length(val) != 1) {
#           val <- capture.output(str(val))
#           val <- paste(collapse = '\n', val)
#         } else {
#           pre <- FALSE
#         }
#         val <- gsub('<', '&lt;', val, fixed = TRUE)
#         val <- gsub('>', '&gt;', val, fixed = TRUE)
#         if (pre) val <- paste0('<pre>', val, '</pre>')
#         paste0('<small><code>', val, '</code></small>')
#       }
#       
#       rows <- paste(collapse = '', sapply(sort(ls(req)), function (nm) {
#         paste0('<tr><td><small>', nm, '</small></td><td>', escape(nm), '</td></tr>') }))
#       
#       header <- "Content of this HTTP Request"
#       text   <- paste0('<table>', rows, '</table>')
#       
#     } else if (startsWith(page, '/sleep')) {
#       
#       wait <- ifelse(is.null(req$ARGS$wait), 0, as.integer(req$ARGS$wait))
#       
#       t1 <- as.character(Sys.time())
#       Sys.sleep(wait)
#       t2 <- as.character(Sys.time())
#       
#       header <- paste('Waited', wait, 'seconds')
#       text   <- paste0(
#         '<pre>',
#         '<code>',
#         'Started:  ', t1, '\n',
#         'Finished: ', t2,
#         '</code>',
#         '</pre>',
#         '<hr>',
#         form('wait', 'number', wait, 'Sleep'),
#         'If more than the maximum of 10 seconds, the job will be automatically stopped before finishing.')
#       
#       
#     } else {
#       header <- "Welcome to WebQueue"
#       text   <- "Select a link above to explore what R's request handler can see and do."
#     }
#     
#     html <- template(header, text)
#     return (html)
#   })
#   
#   # Ignore requests for a website fav icon.
#   hook <- function (job, queue) {
#     if (job$vars$req$PATH_INFO == '/favicon.ico') job$stop(404L)
#   }
#   
#   
#   wq <- web_queue(
#     handler = handler, 
#     host    = '127.0.0.1', 
#     port    = 8080L, 
#     timeout = 10, 
#     hook    = hook,
#     stop_id = ~{ .$GET$stop_id }, 
#     copy_id = ~{ .$GET$copy_id }, 
#     globals = globals, 
#     workers = 4L )
#   
#   cli_text("Site available at {.url http://127.0.0.1:8080}")
#   
#   return (wq)
# }
# 
