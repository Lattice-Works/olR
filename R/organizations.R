library(RPostgres)
library(dbplyr)
library(DBI)

get_api <- function(jwt, api, local = FALSE) {
    if (local) {
        basepath = "http://localhost:8080"
    } else {
        basepath = "https://api.openlattice.com"
    }
    header_params = unlist(list("Authorization" = paste("Bearer", jwt)))
    client <- ApiClient$new(defaultHeaders = header_params,
                            basePath = basepath)
    thisApi <- api$new(apiClient = client)
    return(thisApi)
}

get_con <- function(orgid, jwt, local = FALSE) {
  if (is.null(orgid)) {
    return (NA)
  }
  if (is.na(orgid)) {
        return (NA)
    }
    if (TRUE) {
      orgApi <- get_api(jwt, OrganizationsApi, local=local)
      credentials = orgApi$get_organization_integration_account(orgid)
      usernm = credentials$user
    } else  {
      prinApi <- get_api(jwt, PrincipalApi, local=local)
      credentials = prinApi$get_materialized_view_account()
      usernm = credentials$username
    }
  orgid = paste0("org_", str_replace_all(orgid, "-", ""))
  
    check <- DBI::dbCanConnect(
        RPostgres::Postgres(),
        dbname = orgid,
        host = ifelse(local, "localhost", "atlas.openlattice.com"),
        port = ifelse(local, 5432, 30001),
        password = credentials$credential,
        user = usernm
    )
    if (check) {
        con <- DBI::dbConnect(
            RPostgres::Postgres(),
            dbname = orgid,
            host = ifelse(local, "localhost", "atlas.openlattice.com"),
            port = ifelse(local, 5432, 30001),
            password = credentials$credential,
            user = usernm
        )
        return(con)
    }
}


get_external_tables <- function(jwt, orgid){
    datasetApi = get_api(jwt, DatasetApi, local = TRUE)
    tables = datasetApi$get_external_database_tables(orgid) %>%
        transform_object_to_tibble() %>%
        select(id, name, title) %>%
        rename(tableId = id, tableName = name, tableTitle = title)
    table_columns = datasetApi$get_external_database_tables_with_columns(orgid) %>%
        transform_deep_object_to_tibble() %>%
        left_join(tables)
    return(table_columns)
}


get_organizations <- function(jwt, local=FALSE) {
    orgApi <- get_api(jwt, OrganizationsApi, local=local)
    orgs = orgApi$get_organizations()
    if (typeof(orgs) == 'environment') {
        return (tibble())
    }
    
    orgs = transform_object_to_tibble(orgs)[c('id', 'title', 'description')]
    return(orgs)
}

get_datatype_query <- function(table_name, column_name, data_type) {
    query = paste0(
        "ALTER TABLE ",
        table_name,
        " ALTER COLUMN \"",
        column_name,
        "\" SET DATA TYPE ",
        data_type,
        " USING \"",
        column_name,
        "\"::",
        data_type
    )

    return (query)
}

upload_table <- function(con, table_name, data_table, schema = "openlattice"){
  if(table_name == ""){table_name = paste0("dataset_", str_replace_all(today(), "-", ""))}
  table_name = str_replace(table_name, " ", "_")
  table_name = ifelse (!str_detect(table_name, schema), paste0(schema,".", table_name), table_name)
  dbWriteTable(con, SQL(table_name), data_table, overwrite=TRUE)
  
}

get_preview <- function(con, table_name){
  return(tbl(con, table_name) %>% head(10) %>% collect())
}

list_tables <- function(con, schema = "openlattice"){
    query = paste0("SELECT n.nspname as \"Schema\",
  c.relname as \"Name\",
  pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\",
  pg_catalog.pg_size_pretty(pg_catalog.pg_table_size(c.oid)) as \"Size\"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r','p','s','')
      AND n.nspname !~ '^pg_toast'
  AND n.nspname OPERATOR(pg_catalog.~) '^(", schema,")$'
ORDER BY 1,2;")
    rs = dbSendQuery(con, query)
    out = dbFetch(rs) %>% as_tibble()
    dbClearResult(rs)
    
    
    return(out)
}
