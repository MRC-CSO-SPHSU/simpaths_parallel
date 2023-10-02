library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(stringr)
library(purrr)
library(furrr)

source("./schemas/schemas.R")

missing_values <- c("",
                    "NA",
                    "Na",
                    "na",
                    "NULL",
                    "Null",
                    "null",
                    "NAN",
                    "NaN",
                    "Nan",
                    "nan")

#' Extracts a particular substring of a string containing the underscore symbol
#'
#' @description `get.batch_id` splits a string containing an single underscore
#' into two parts, before and after it
#'
#' @details It is assumed there is one and only one underscore symbol in the
#' input string, there are no checks for all other cases. This function is
#' designed to help with unconventional file names
#'
#' @param x A string
#'
#' @return The second element of the substring array
#'
#' @examples
#' get.batch_id("1_2")
get.batch_id <- function(x) {
  batch_id <- x |> str_split_1(pattern = "_")
  gsub("00", "", batch_id[[2]]) |> as.integer()
}

#' Converts simulation results from `*.csv` files to `*.parquet`.
#'
#' @description `convert.single_simulation` results of a single simulation run:
#' `Household.csv`, `Person.csv`, `BenefitUnit.csv` to binary files for further
#' use with Apache Arrow. All data is grouped by the corresponding
#' `scenario_name`, `year` and `run_id`.
#'
#' @details This function is designed for efficient data processing and therefore
#' doesn't deal with `*.csv` files that contain trailing commas & spaces. All
#' preprocessing MUST be done in advance.
#' https://unix.stackexchange.com/questions/220576/how-to-remove-last-comma-of-each-line-on-csv-using-linux
#'
#' @param path_string The name of a directory containing simulation results
#' @param scenario_id The string description of a dataset. Typically that would
#' be "baseline" or "reform", but there is no hard restrictions
#' @param files A list of strings that represent file names
#' @param delete_original A flag to ensure that the original files are (not)
#' deleted, `FALSE` by default
#'
#' @seealso `convert.multiple_simulations`
#'
#' @examples
#' convert.single_simulation("2023121212_100", "baseline")
convert.single_simulation <- function(path_string,
                                      scenario_id,
                                      files = c("Household",
                                                "Person",
                                                "BenefitUnit"),
                                      delete_original = FALSE) {
  batch_id <- get.batch_id(path_string)
  
  f.base <- scenario_id |>
    paste("output", sep = "/") |>
    paste(path_string, sep = "/") |>
    paste("csv", sep = "/")
  
  f.person <- f.base |>
    paste("Person", sep = "/") |>
    paste("csv", sep = ".")
  
  f.household <- f.base |>
    paste("Household", sep = "/") |>
    paste("csv", sep = ".")
  
  f.benefitunit <- f.base |>
    paste("BenefitUnit", sep = "/") |>
    paste("csv", sep = ".")
  
  f.person |>
    open_csv_dataset(
      schema = schema.person,
      skip = 1,
      na = missing_values,
      convert_options = CsvConvertOptions$create(
        null_values = missing_values,
        strings_can_be_null = TRUE,
        true_values = c("true", "TRUE", "True", "Male"),
        false_values = c("false", "FALSE", "False", "Female")
      )
    ) |>
    mutate(
      time = cast(time, uint16()),
      countFemale = cast(countFemale, uint8()),
      countMale = cast(countMale, uint8())
    ) |>
    rename(id_BenefitUnit = idBenefitUnit) |>
    collect() |>
    mutate(
      labourSupplyWeekly = case_when(
        labourSupplyWeekly == "ZERO" ~ "0",
        labourSupplyWeekly == "TEN" ~ "10",
        labourSupplyWeekly == "TWENTY" ~ "20",
        labourSupplyWeekly == "THIRTY" ~ "30",
        labourSupplyWeekly == "FORTY" ~ "40"
      ),
      labourSupplyWeekly = as.integer(labourSupplyWeekly)
    ) |>
    as_arrow_table(schema = schema.person.save) |>
    mutate(
      scenario = scenario_id,
      run = run + (batch_id - 1) * 20,
      run = cast(run, uint16())
    ) |>
    group_by(scenario, time, run) |>
    write_dataset(path = "Person",
                  format = "parquet",
                  compression = 'lz4')
  
  
  f.benefitunit |>
    open_csv_dataset(
      schema = schema.benefit_unit,
      skip = 1,
      na = missing_values,
      convert_options = CsvConvertOptions$create(
        null_values = missing_values,
        strings_can_be_null = TRUE,
        true_values = c("true", "TRUE", "True", "Male"),
        false_values = c("false", "FALSE", "False", "Female")
      )
    ) |>
    mutate(
      time = cast(time, uint16()),
      atRiskOfPoverty = cast(atRiskOfPoverty, bool()),
      scenario = scenario_id,
      run = run + (batch_id - 1) * 20,
      run = cast(run, uint16())
    ) |>
    group_by(scenario, time, run) |>
    write_dataset(path = "BenefitUnit",
                  format = "parquet",
                  compression = 'lz4')
  
  
  f.household |>
    open_csv_dataset(
      schema = schema.household,
      skip = 1,
      na = missing_values,
      convert_options = CsvConvertOptions$create(
        null_values = missing_values,
        strings_can_be_null = TRUE,
        true_values = c("true", "TRUE", "True", "Male"),
        false_values = c("false", "FALSE", "False", "Female")
      )
    ) |>
    mutate(
      time = cast(time, uint16()),
      scenario = scenario_id,
      run = run + (batch_id - 1) * 20,
      run = cast(run, uint16())
    ) |>
    group_by(scenario, time, run) |>
    write_dataset(path = "Household",
                  format = "parquet",
                  compression = 'lz4')
  
  
  if (delete_original) {
    file.remove(f.person)
    file.remove(f.household)
    file.remove(f.benefitunit)
  }
}

#' Converts all `*.csv` files stored in a directory to `*.parquet`
#'
#' @param scenario_names A list of scenario names
#' @param paths_pattern A regular expression that allows to select specific
#' subdirectories only
#'
#' @seealso `convert.single_simulation`
convert.multiple_simulations <- function(scenario_names = c("baseline", "reform"),
                                         paths_pattern = "^2023\\d+\\_([1-9]|[1234][0-9]|50)00$") {
  for (sid in scenario_names) {
    paths <- sid |> paste("output", sep = "/") |> dir()
    paths[str_detect(paths, paths_pattern)] |>
      map(\(x) convert.single_simulation(x, sid), .progress = TRUE)
  }
}

convert.multiple_simulations()

