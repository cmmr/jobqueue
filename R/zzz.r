

# nocov start
ENV <- new.env(parent = emptyenv())

.onLoad <- function (libname, pkgname) {
  ENV$pid    <- p__ps_string()
  ENV$jq_dir <- dir_create(tempfile('jq'))
}
# nocov end
