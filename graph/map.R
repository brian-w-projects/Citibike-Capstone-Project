# Code adapted from https://rpubs.com/jhofman/nycmaps
library(tidyverse)
library(tigris)
library(leaflet)
library(ggmap)
library(httr)
library(rgdal)
library(broom)
library(sp)

register_google(key= key)
nyc_map <- get_map(location = c(lon = -74.00, lat = 40.71), maptype = "terrain", zoom = 11)
r <- GET('http://data.beta.nyc//dataset/0ff93d2d-90ba-457c-9f7e-39e47bf2ac5f/resource/35dd04fb-81b3-479b-a074-a27a37888ce7/download/d085e2f8d0b54d4590b1e7d1f35594c1pediacitiesnycneighborhoods.geojson')
nyc_neighborhoods <- readOGR(content(r,'text'), 'OGRGeoJSON', verbose = F)


markers.plot <- function(file, threshold=50){
  csv <- read_csv(file) %>%
    filter(row_number() <= threshold)
  nyc_map %>%
    leaflet() %>%
    addMarkers(~lon, ~lat, data=csv) %>%
    addProviderTiles("CartoDB.Positron")
}

areas.plot <- function(file){
  data <- match.neighborhoods(file)
  color <- color.creator(data)
  markers <- create.markers(data)
  
  map_data <- geo_join(nyc_neighborhoods, color, 'neighborhood', 'neighborhood')
  palette <- colorNumeric(palette='RdBu', domain=range(map_data@data$n, na.rm=T))
  
  map_data %>%
    leaflet() %>%
    addTiles() %>%
    addPolygons(fillColor= ~palette(n), opacity=0.1, weight=1) %>%
    addMarkers(~lon, ~lat, data=markers) %>%
    addProviderTiles("CartoDB.Positron")
}


file <- read_csv('high_ratio.csv') %>%
  arrange(ratio)
areas.plot(file)

create.markers <- function(data, threshold=15){
  markers <- data %>%
    filter(row_number() <= threshold)
  return(markers)
}

match.neighborhoods <- function(data){
  points <- data
  coordinates(points) <- ~lon + lat
  proj4string(points) <- proj4string(nyc_neighborhoods)
  matches <- over(points, nyc_neighborhoods)
  return(cbind(data, matches))
}

color.creator <- function(data, threshold=75){
  color <- data %>%
    filter(row_number() <= threshold) %>%
    group_by(neighborhood) %>%
    count()
  return(color)
}
