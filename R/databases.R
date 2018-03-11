#' Extract SQL query as string from lazy query.
#'
#' @param lazy_qry A `tbl_sql` object
#'
#' @return Character string representing SQL command.
#' @export
#' @name sql_command
sql_command <- function(lazy_qry) {
  UseMethod("sql_command")
}


#' @rdname sql_command
#' @export
sql_command.tbl_sql <- function(lazy_qry) {

  cmnd <- dbplyr::db_sql_render(lazy_qry$src$con, lazy_qry) %>%
    stringr::str_replace_all("<SQL> ", "") %>%
    stringr::str_replace_all('\"', "") %>%
    stringr::str_replace_all("\n", " ")

  cmnd

}


#' List all materialized views found in Postgres connection
#'
#' @param src A database connection (e.b. DBIConnection)
#' @param schema Name of database schema in which to look
#'
#' @return A character vector containing names of materialized views found.
#' @export
listMatViews <- function(src, schema = "public") {
  UseMethod("listMatViews")
}

#' @rdname listMatViews
#' @export
listMatViews.src_dbi <- function(src, schema = "public") {
  listMatViews(src$con, schema)
}

#' @rdname listMatViews
#' @export
listMatViews.DBIConnection <- function(src, schema = "public") {
  ret <- odbc::dbGetQuery(src,
                          sprintf(
                            "SELECT matviewname FROM pg_matviews WHERE schemaname = '%s';",
                            schema
                          ))
  ret <- unlist(ret)
  names(ret) <- NULL
  ret

}


#' List all (temporary) views found in Postgres connection
#'
#' @param src A data source
#' @param schema Name of database schema in which to look
#'
#' @return A character vector containing names of views found.
#' @export
listViews <- function(src, schema = "public") {
  UseMethod("listViews")
}

#' @rdname listViews
#' @export
listViews.src_dbi <- function(src, schema = "public") {
  listViews(src$con, schema)
}

#' @rdname listViews
#' @export
listViews.DBIConnection <- function(src, schema = "public") {
  ret <- DBI::dbGetQuery(src,
                         sprintf("SELECT viewname FROM pg_views WHERE schemaname = '%s';",
                                 schema))
  ret <- unlist(ret)
  names(ret) <- NULL
  ret

}



#' List all tables found in the corresponding schema in the connection `src`
#'
#' @param src A data source
#' @param schema Name of database schema in which to look
#'
#' @return A character vector containing names of tablesfound.
#' @export
#'
listTables <- function(src, schema = "public") {
  UseMethod("listTables")
}

#' @rdname listViews
#' @export
listTables.src_dbi <- function(src, schema = "public") {
  listTables(src$con, schema)
}

#' @rdname listTables
#' @export
listTables.DBIConnection <- function(src, schema = "public") {
  ret <- DBI::dbGetQuery(src,
                         sprintf(
                           "SELECT tablename FROM pg_tables WHERE schemaname = '%s';",
                           schema
                         ))
  ret <- unlist(ret)
  names(ret) <- NULL
  ret

}

#' Identify all Tables & Views In a Database Connection
#'
#' @param src A data source
#' @param schema Name of database schema in which to look
#'
#' @return A named character vector
#' @export
tableTypes <- function(src, schema = "public") {
  UseMethod("tableTypes")
}

#' @rdname tableTypes
#' @export
tableTypes.src_dbi <- function(src, schema = "public") {
  tableTypes(src$con, schema)
}

#' @rdname tableTypes
#' @export
tableTypes.DBIConnection <- function(src, schema = "public") {
  tables <- listTables(src, schema)
  views <- listViews(src, schema)
  mat_views <- listMatViews(src, schema)

  ret <- c(rep("table", length(tables)),
           rep("view", length(views)),
           rep("materialized view", length(mat_views)))

  names(ret) <- c(tables, views, mat_views)
  ret
}

#' Check to see if a Table/View exists in a DB source.
#'
#' If the table exists, prompts user for whether they want it dropped,
#'   with the additional option of cascading to any dependent views.
#'
#' @param src A data source / DB connection
#' @param name Name of table or view
#' @param schema Name of database schema in which to look
#'
#' @return TRUE if table does not exist; FALSE otherwise
#' @export
dbCheckTable <- function(src, name, schema = "public") {
  UseMethod("dbCheckTable")
}

#' @rdname dbCheckTable
#' @export
dbCheckTable.src_dbi <- function(src, name, schema = "public") {
  dbCheckTable(src$con, name, schema)
}

#' @rdname dbCheckTable
#' @export
dbCheckTable.DBIConnection <-
  function(src, name, schema = "public") {
    tbl_types <- tableTypes(src, schema)

    if (name %in% names(tbl_types)) {
      tbl_type <- tbl_types[name]

      drop <- utils::menu(
        c("Yes", "No"),
        title = sprintf(
          "%s %s already exists. Would you like to drop it?",
          tbl_type,
          name
        )
      )

      if (drop == 1) {
        result <- tryCatch({
          cmnd <- sprintf("DROP %s %s;", tbl_type, name)
          message(sprintf('Running command: "%s"', cmnd))
          rslt <- DBI::dbExecute(src, cmnd)
          if (rslt) {
            message(sprintf("%s successfully dropped!", name))
            return(TRUE)
          }
        },
        error = function(e) {
          message(sprintf(
            "cannot drop %s %s because other objects depend on it",
            tbl_type,
            name
          ))
          cascade <- utils::menu(c("Yes", "No"), title = "Cascade?")

          if (cascade == 1) {
            cmnd <- sprintf("DROP %s %s CASCADE;", tbl_type, name)
            message(sprintf('Running command: "%s"', cmnd))
            rslt <- DBI::dbExecute(src, cmnd)
            if (rslt) {
              message(sprintf("%s successfully dropped!", name))
              return(TRUE)
            }
          }
        })
      }
      return(FALSE)
    }
    return(TRUE)
  }


#' Create a new schema
#'
#' @param schema name of schema
#' @param db (optional) A Database
#'
#' @export
create_schema <- function(schema, db = NULL) {
  UseMethod("create_schema")
}

#' @rdname create_schema
#' @export
create_schema.character <- function(schema, db = NULL) {
  db <- check_db(db)
  cmnd <- sprintf("CREATE SCHEMA IF NOT EXISTS %s;", schema)
  rslt <- DBI::dbExecute(db$con, cmnd)
  return(rslt)
}

#' Create a materialized view from a lazy `tbl_dbi`
#'
#' @param ... Name-value pairs of expressions. The name will be the name of the
#'   materialized view, and the expression should be the name of the `tbl_dbi`
#' @param .checks Perform checks to make sure materialized view doesn't already
#'   exist?
#' @param schema database schema in which to place
#'
#' @export
create_mtrl_views <- function(..., .checks = TRUE, schema = "public") {
  create_db_tbls(...,
                 type = "materialized view",
                 .checks = TRUE,
                 schema = schema)
}


#' Create a materialized view from a lazy `tbl_dbi`
#'
#' @param ... Name-value pairs of expressions. The name will be the name of the
#'   materialized view, and the expression should be the name of the `tbl_dbi`
#' @param .checks Perform checks to make sure materialized view doesn't already
#'   exist?
#' @param schema database schema in which to place
#'
#' @export
create_views <- function(..., .checks = TRUE, schema = "public") {
  create_db_tbls(...,
                 type = "view",
                 .checks = TRUE,
                 schema = schema)
}


#' Create a table from a lazy `tbl_dbi`
#'
#' @param ... Name-value pairs of expressions. The name will be the name of the
#'   materialized view, and the expression should be the name of the `tbl_dbi`
#' @param .checks Perform checks to make sure materialized view doesn't already
#'   exist?
#' @param schema database schema in which to place
#'
#' @export
create_tables <- function(..., .checks = TRUE, schema = "public") {
  create_db_tbls(...,
                 type = "table",
                 .checks = TRUE,
                 schema = schema)
}


#' Create a table inside a database connection
#'
#' @param ... Name-value pairs of expressions. The name will be the name of the
#'   materialized view, and the expression should be the name of the `tbl_dbi`
#' @param type The type of table/view to create
#' @param .checks Perform checks to make sure materialized view doesn't already
#'   exist?
#' @param schema database schema in which to place
#'
#' @export
create_db_tbls <-
  function(...,
           type = c("view", "materialized view", "table"),
           .checks = TRUE,
           schema = "public") {

    type <- match.arg(type)
    dots <- rlang::quos(..., .named = TRUE)
    if (!length(dots))
      stop("No arguments!!!")

    ret <- lapply(seq_along(dots),
                  function(exprs, nms, ii) {
                    create_db_tbl(
                      x = rlang::eval_tidy(exprs[[ii]]),
                      name = nms[[ii]],
                      type = type,
                      .checks = .checks,
                      schema = schema
                    )
                  },
                  exprs = dots,
                  nms = names(dots))

  }


#' Create a table inside a database connection
#'
#' @param x An object representing the table to be created in the database
#' @param name The name of the table/view
#' @param type What type of view should be created in the database?
#' @param .checks Should we check for the existence of an object in the database
#'   with `name` {name}?
#' @param schema schema
#'
#' @return Logical indicating whether the view was successfully created.
#' @export
create_db_tbl <- function(x,
                          name,
                          type = c("view", "materialized view", "table"),
                          .checks = TRUE,
                          schema = "public") {
  UseMethod("create_db_tbl")
}


#' @rdname create_db_tbl
#' @export
create_db_tbl.tbl_sql <- function(x,
                                  name,
                                  type = c("view", "materialized view", "table"),
                                  .checks = TRUE,
                                  schema = "public") {

  type <- match.arg(type)
  src <- x$src

  cmnd <- sprintf("CREATE %s %s.%s AS (%s);",
                  toupper(type),
                  schema,
                  name,
                  sql_command(x))

  commit <- TRUE

  if (.checks)
    commit <- dbCheckTable(src, name)

  if (commit) {
    con <- src$con
    message(sprintf('Creating %s "%s.%s"', type, schema, name))
    rslt <- DBI::dbExecute(con, cmnd)
    if (rslt) {
      message(sprintf("%s successfully created!", name))
      return(TRUE)
    }
  }
}


#' Check to see if object is a valid database connection
#'
#' @param dbObj object to be tested
#' @param ... parameters passed on to `DBI::dbIsValid``
#'
#' @return `TRUE` if `dbObj` is a valid database
#' @export
dbIsValid <- function(dbObj, ...) {
  UseMethod("dbIsValid")
}

#' @export
#' @rdname dbIsValid
dbIsValid.default <- function(dbObj, ...) {
  DBI::dbIsValid(dbObj, ...)
}

#' @export
#' @rdname dbIsValid
dbIsValid.src_dbi <- function(dbObj, ...) {
  DBI::dbIsValid(dbObj$con, ...)
}

#' Load Database connections into Global Environment
#'
#' @param name (OPTIONAL) name of ODBC database source
#'
#' @return Copy of loaded database
#' @export
load_db_deprecated <- function(name = NULL) {

  if (is.null(name)) {
    data_srcs <- odbc::odbcListDataSources()

    if (!NROW(data_srcs))
      stop("Cant find any data sources.")

    choice <- utils::menu(choices = data_srcs$name,
                          title = "Select your data source: ")

    if (!choice)
      stop("No database selected. Stopping.")

    name <- data_srcs$name[choice]
  }

  if (!(is.character(name) && name %in% odbc::odbcListDataSources()$name))
    stop("Please pass the name of a valid database.")

  con <- odbc::dbConnect(odbc::odbc(),
                         dsn = name)
  db <- dbplyr::src_dbi(con, auto_disconnect = TRUE)
  # assign(".db", db, envir = .GlobalEnv)
  return(db)
}


#' Load Database connections into Global Environment
#'
#' @param dbname name of ODBC database source
#' @param config_file path of configuration file
#'
#' @return Copy of loaded database
#' @export
load_db <- function(dbname, config_file = NULL) {
  if (is.null(config_file))
    config_file <- "~/.stride/config.yml"

  checkmate::assert_file_exists(config_file)

  config <- configr::read.config(file = config_file)

  checkmate::assert_choice(dbname, names(config))

  dbconfig <- config[[dbname]]

  dplyr::src_postgres(
    dbname = dbconfig[["database"]],
    host = dbconfig[["host"]],
    user = dbconfig[["user"]],
    password = dbconfig[["passwd"]],
    port = dbconfig[["port"]]
  )

}


#' Load all tables (all types) into Global environment
#'
#' @param db (optional) A Database
#'
#' @export
load_tables <- function(db = NULL) {
  db <- check_db(db)
  load_base_tables(db)
  load_views(db)
  load_materialized_views(db)
}

check_db <- function(db = NULL) {
  if (is.null(db)) {
    if (is.null(db <- get0('.db', .GlobalEnv)))
      db <- load_db_deprecated()
    return(db)
  } else {
    if (methods::is(db, "src_dbi"))
      return(db)
    else if (methods::is(db, "DBIConnection")) {
      db <- dbplyr::src_dbi(db, auto_disconnect = TRUE)
      return(db)
    }
    else {
      return(FALSE)
    }
  }
}

load_tables_helper <- function(db, tblnames) {
  thisenv <- new.env()
  for (tblname in tblnames) {
    assign(tblname,
           dplyr::tbl(db, tblname),
           envir = thisenv)
  }
  return(thisenv)

}


#' Load all tables into Global environment
#'
#' @param db (optional) A Database
#'
#' @export
load_base_tables <- function(db = NULL) {
  db <- check_db(db)
  env <- load_tables_helper(db, listTables(db))
}


#' Load all views into Global environment
#'
#' @param db (optional) A Database
#'
#' @export
load_views <- function(db = NULL) {
  db <- check_db(db)
  env <- load_tables_helper(db, listViews(db))
}

#' Load all materialized views into Global environment
#'
#' @param db (optional) A Database
#'
#' @export
load_materialized_views <- function(db = NULL) {
  db <- check_db(db)
  env <- load_tables_helper(db, listMatViews(db))
}

#' Refresh all materialized views
#'
#' @param db (optional) A Database
#' @export
refresh_materlialized_views <- function(db = NULL) {
  db <- check_db(db)
  mtrl_views <- listMatViews(db)

  for (view in mtrl_views) {
    cmnd <- sprintf("REFRESH MATERIALIZED VIEW %s;",
                    view)
    rslt <- DBI::dbExecute(db$con, cmnd)
    if (rslt)
      message(sprintf("%s successfully refreshed!", view))
  }

}
