% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/util.R
\name{datatable}
\alias{datatable}
\title{Create an HTML table widget using the DataTables library}
\usage{
datatable(data, align = NULL, nrows = NULL, options = list(), ...)
}
\arguments{
\item{data}{a data object (either a matrix or data frame)}

\item{align}{a character vector detailing how columns should be aligned}

\item{nrows}{how many rows per page should be displayed}

\item{options}{a list of initialization options}

\item{...}{additional options passed on to \code{\link[DT]{datatable}}}
}
\value{
A \code{\link[DT]{datatable}}
}
\description{
Custom edit to datatable function from DT package to allow for easier column alignment
}
