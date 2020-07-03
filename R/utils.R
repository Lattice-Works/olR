transform_list_to_tibble <- function(data){
    unlisted = lapply(data, function(x) x %>% unlist())
    data = as_tibble(invert_list(unlisted)) %>% mutate_all(unlist)
    return(data)
}

transform_object_to_tibble <- function(data){
    unlisted = lapply(data, function(x) x$toJSON() %>% unlist())
    data = as_tibble(invert_list(unlisted)) %>% mutate_all(unlist)
    return(data)
}

transform_deep_object_to_tibble <- function(data){
    unlisted_l1 = lapply(data, transform_object_to_tibble)
    data = bind_rows(unlisted_l1)
    return(data)
}

invert_list <-  function(ll) { # @Josh O'Brien
    nms <- unique(unlist(lapply(ll, function(X) names(X))))
    ll <- lapply(ll, function(X) setNames(X[nms], nms))
    ll <- apply(do.call(rbind, ll), 2, as.list)
    lapply(ll, function(X) X[!sapply(X, is.null)])
}