#' @importFrom dplyr %>%
#' @export
dplyr::`%>%`

#' @importFrom magrittr %<>%
#' @export
magrittr::`%<>%`

#' Get full path of the current script file
#'
#' @return The full path of current script file
#' @importFrom rstudioapi getActiveDocumentContext
#' @export
script_path <- function() {
  # http://stackoverflow.com/a/32016824/2292993
  cmdArgs = commandArgs(trailingOnly = FALSE)
  needle = "--file="
  match = grep(needle, cmdArgs)
  if (length(match) > 0) {
    # Rscript via command line
    return(normalizePath(sub(needle, "", cmdArgs[match])))
  } else {
    ls_vars = ls(sys.frames()[[1]])
    if ("fileName" %in% ls_vars) {
      # Source'd via RStudio
      return(normalizePath(sys.frames()[[1]]$fileName))
    } else {
      if (!is.null(sys.frames()[[1]]$ofile)) {
        # Source'd via R console
        return(normalizePath(sys.frames()[[1]]$ofile))
      } else {
        # RStudio Run Selection
        # http://stackoverflow.com/a/35842176/2292993
        return(normalizePath(rstudioapi::getActiveDocumentContext()$path))
      }
    }
  }
}

#' Create an HTML table widget using the DataTables library
#'
#' Custom edit to datatable function from DT package to allow for easier column alignment
#'
#' @param data a data object (either a matrix or data frame)
#' @param align a character vector detailing how columns should be aligned
#' @param nrows how many rows per page should be displayed
#' @param options a list of initialization options
#' @param ... additional options passed on to \code{\link[DT]{datatable}}
#' @return A \code{\link[DT]{datatable}}
#' @importFrom DT datatable
#' @export
datatable <- function(data, align = NULL, nrows = NULL, options = list(), ...) {

  if (!missing(align) && length(align) == 1L)
    align <- strsplit(align, "")[[1]]

  if (length(align) && !all(align %in% c("l", "r", "c")))
    stop("'align' must be a character vector of possible values 'l', 'r', and 'c'")

  if (length(nrows) == 1) {
    options[['pageLength']] <- nrows
  }

  if (length(align)) {
    options[['columnDefs']] <- lapply(
      unique(align),
      function(x) {
        list(
          className = switch(x,
                             l = 'dt-left',
                             c = 'dt-center',
                             r = 'dt-right'),
          targets = which(align == x) - 1
        ) # close list
      } # close anon function
    ) # close lapply
  } # close if

  return(DT::datatable(data, options, ...))
}

#' Install or Load a list of Packages
#'
#' @param packages a character vector of package names
#' @importFrom utils installed.packages install.packages
#' @export
load_packages <- function(packages) {
  new_packages <- packages[!(packages %in% installed.packages()[,"Package"])]
  if (length(new_packages)) install.packages(new_packages)

  lapply(packages, require, character.only = TRUE)
}
