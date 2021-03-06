% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/databases.R
\name{dbIsValid}
\alias{dbIsValid}
\alias{dbIsValid.default}
\alias{dbIsValid.src_dbi}
\title{Check to see if object is a valid database connection}
\usage{
dbIsValid(dbObj, ...)

\method{dbIsValid}{default}(dbObj, ...)

\method{dbIsValid}{src_dbi}(dbObj, ...)
}
\arguments{
\item{dbObj}{object to be tested}

\item{...}{parameters passed on to `DBI::dbIsValid``}
}
\value{
`TRUE` if `dbObj` is a valid database
}
\description{
Check to see if object is a valid database connection
}
