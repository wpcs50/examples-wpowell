library(tidyverse)
library(sf)
library(here)

'%!in%' <- function(x,y)!('%in%'(x,y))

#######################################
#### Edit contents of shp directory
######################################
TAZs <- here("model",
             "inputs",
             "zonal",
             "shp",
             "CTPS_TDM23_TAZ_2017g_v202303.shp") |>
  st_read() |>
  st_transform("WGS84") |>
  filter(mpo %!in% c("BRPC", "PVPC", "FRCOG"))

block_alloc <- here("model",
                    "inputs",
                    "zonal",
                    "shp",
                    "taz_2010block_allocation_20230314.csv") |>
  read_csv(show_col_types = FALSE) |>
  filter(taz_id %in% TAZs$taz_id)

block_assign <- here("model",
                     "inputs",
                     "zonal",
                     "shp",
                     "taz_2010block_assignment_20230314.csv") |>
  read_csv(show_col_types = FALSE) |>
  filter(taz_id %in% TAZs$taz_id)

TAZ_puma <- here("model",
                 "inputs",
                 "zonal",
                 "shp",
                 "tazpuma.csv") |>
  read_csv(show_col_types = FALSE) |>
  filter(taz_id %in% TAZs$taz_id)

#####################################
## Write contents of shp directory
###################################
st_write(TAZs, 
         here("model",
              "inputs",
              "zonal_reduce",
              "shp",
              "TAZs.shp"),
         append = FALSE)

write_csv(block_alloc,
          here("model",
               "inputs",
               "zonal_reduce",
               "shp",
               "block_allocation.csv"))

write_csv(block_assign,
          here("model",
               "inputs",
               "zonal_reduce",
               "shp",
               "block_assignment.csv"))

write_csv(TAZ_puma,
          here("model",
               "inputs",
               "zonal_reduce",
               "shp",
               "tazpuma.csv"))

#################################################
## Edit rest of zonal files
#############################################

ma_pop <- here("model",
                "inputs",
                "zonal",
                "ma_population_run97-176_2019_v20240109.csv") |>
  read_csv(show_col_types = FALSE) |>
  filter(block_id %in% block_assign$block_id) 

ma_emp <- here("model",
               "inputs",
               "zonal",
               "ma_employment_run97-176_2019_v20240109.csv") |>
  read_csv(show_col_types = FALSE) |>
  filter(block_id %in% block_assign$block_id) 

walkbike <- here("model",
                 "inputs",
                 "zonal",
                 "walkbike_v20220411.csv") |>
  read_csv(show_col_types = FALSE) |>
  filter(taz_id %in% TAZs$taz_id)

enroll <- here("model",
               "inputs",
               "zonal",
               "enroll_v20221028.csv") |>
  read_csv(show_col_types = FALSE) |>
  filter(taz_id %in% TAZs$taz_id)

parking <- here("model",
               "inputs",
               "zonal",
               "parking_v20221007.csv") |>
  read_csv(show_col_types = FALSE) |>
  filter(taz_id %in% TAZs$taz_id)

#################################################
## Write rest of zonal files
#############################################

write_csv(ma_pop,
          here("model",
               "inputs",
               "zonal_reduce",
               "ma_population.csv"))

write_csv(ma_emp,
          here("model",
               "inputs",
               "zonal_reduce",
               "ma_employment.csv"))

write_csv(walkbike,
          here("model",
               "inputs",
               "zonal_reduce",
               "walkbike.csv"))

write_csv(enroll,
          here("model",
               "inputs",
               "zonal_reduce",
               "enroll.csv"))

write_csv(parking,
          here("model",
               "inputs",
               "zonal_reduce",
               "parking.csv"))
