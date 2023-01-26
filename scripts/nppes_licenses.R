

#  ------------------------------------------------------------------------
#
# Title : NPPES Clickhouse Project
#    By : Andrew Bruce
#  Date : 2023-01-25
#
#  ------------------------------------------------------------------------

library(here)
options(scipen = 999)

# header file -------------------------------------------------------------
nppes_cols <- vroom::vroom(here("data_raw/errata", "npidata_pfile_20050523-20230108_fileheader.csv"))

# vector of new clean column names
nppes_cols <- nppes_cols |> janitor::clean_names() |> colnames()

# -------------------------------------------------------------------------
nppes_licenses_1 <- readr::read_csv(
  here("data_raw/NPPES_Data_Dissemination_January_2023", "npidata_pfile_20050523-20230108.csv"),
  col_types = readr::cols(.default = readr::col_character()),
  col_select = c(1, 48:107),
  n_max = 1000000,
  skip = 0,
  name_repair = janitor::make_clean_names)

nppes_txon <- nppes_licenses_1 |>
  dplyr::filter(!is.na(healthcare_provider_taxonomy_code_1)) |>
  dplyr::select(c(npi, dplyr::contains("taxonomy_code"))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "taxonomy_code",
                      names_prefix = "healthcare_provider_taxonomy_code_",
                      values_drop_na = TRUE)

nppes_txon_license <- nppes_licenses_1 |>
  dplyr::filter(!is.na(healthcare_provider_taxonomy_code_1)) |>
  dplyr::select(c(npi, dplyr::num_range("provider_license_number_", 1:15))) |>
  tidyr::pivot_longer(cols = dplyr::contains("license_number"),
                      names_to = "id_set",
                      values_to = "provider_license",
                      values_drop_na = TRUE,
                      names_prefix = "provider_license_number_")

nppes_txon_state <- nppes_licenses_1 |>
  dplyr::filter(!is.na(healthcare_provider_taxonomy_code_1)) |>
  dplyr::select(c(npi, dplyr::num_range("provider_license_number_state_code_", 1:15))) |>
  tidyr::pivot_longer(cols = dplyr::contains("state_code"),
                      names_to = "id_set",
                      values_to = "license_state",
                      values_drop_na = TRUE,
                      names_prefix = "provider_license_number_state_code_")

nppes_licenses_1 <- nppes_txon |>
  dplyr::left_join(nppes_txon_license,
                   by = dplyr::join_by(npi, id_set)) |>
  dplyr::left_join(nppes_txon_state,
                   by = dplyr::join_by(npi, id_set)) |>
  dplyr::select(npi,
                id_set,
                license = provider_license,
                taxonomy = taxonomy_code,
                license_state) |>
  dplyr::filter(!is.na(license))

nppes_licenses_1 |> readr::write_csv(here("data", "nppes_licenses_1.csv"), progress = TRUE)


# -------------------------------------------------------------------------
nppes_licenses_2 <- readr::read_csv(
  here("data_raw/NPPES_Data_Dissemination_January_2023", "npidata_pfile_20050523-20230108.csv"),
  col_types = readr::cols(.default = readr::col_character()),
  col_names = nppes_cols,
  col_select = c(1, 48:107),
  n_max = 1000000,
  skip = 1000000)

nppes_txon <- nppes_licenses_2 |>
  dplyr::filter(!is.na(healthcare_provider_taxonomy_code_1)) |>
  dplyr::select(c(npi, dplyr::contains("taxonomy_code"))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "taxonomy_code",
                      names_prefix = "healthcare_provider_taxonomy_code_",
                      values_drop_na = TRUE)

nppes_txon_license <- nppes_licenses_2 |>
  dplyr::filter(!is.na(healthcare_provider_taxonomy_code_1)) |>
  dplyr::select(c(npi, dplyr::num_range("provider_license_number_", 1:15))) |>
  tidyr::pivot_longer(cols = dplyr::contains("license_number"),
                      names_to = "id_set",
                      values_to = "provider_license",
                      values_drop_na = TRUE,
                      names_prefix = "provider_license_number_")

nppes_txon_state <- nppes_licenses_2 |>
  dplyr::filter(!is.na(healthcare_provider_taxonomy_code_1)) |>
  dplyr::select(c(npi, dplyr::num_range("provider_license_number_state_code_", 1:15))) |>
  tidyr::pivot_longer(cols = dplyr::contains("state_code"),
                      names_to = "id_set",
                      values_to = "license_state",
                      values_drop_na = TRUE,
                      names_prefix = "provider_license_number_state_code_")

nppes_licenses_2 <- nppes_txon |>
  dplyr::left_join(nppes_txon_license,
                   by = dplyr::join_by(npi, id_set)) |>
  dplyr::left_join(nppes_txon_state,
                   by = dplyr::join_by(npi, id_set)) |>
  dplyr::select(npi,
                id_set,
                license = provider_license,
                taxonomy = taxonomy_code,
                license_state) |>
  dplyr::filter(!is.na(license))

nppes_licenses_2 |> readr::write_csv(here("data", "nppes_licenses_2.csv"), progress = TRUE)

# -------------------------------------------------------------------------
nppes_licenses_3 <- readr::read_csv(
  here("data_raw/NPPES_Data_Dissemination_January_2023", "npidata_pfile_20050523-20230108.csv"),
  col_types = readr::cols(.default = readr::col_character()),
  col_names = nppes_cols,
  col_select = c(1, 48:107),
  n_max = 1000000,
  skip = 2000000)

nppes_txon <- nppes_licenses_3 |>
  dplyr::filter(!is.na(healthcare_provider_taxonomy_code_1)) |>
  dplyr::select(c(npi, dplyr::contains("taxonomy_code"))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "taxonomy_code",
                      names_prefix = "healthcare_provider_taxonomy_code_",
                      values_drop_na = TRUE)

nppes_txon_license <- nppes_licenses_3 |>
  dplyr::filter(!is.na(healthcare_provider_taxonomy_code_1)) |>
  dplyr::select(c(npi, dplyr::num_range("provider_license_number_", 1:15))) |>
  tidyr::pivot_longer(cols = dplyr::contains("license_number"),
                      names_to = "id_set",
                      values_to = "provider_license",
                      values_drop_na = TRUE,
                      names_prefix = "provider_license_number_")

nppes_txon_state <- nppes_licenses_3 |>
  dplyr::filter(!is.na(healthcare_provider_taxonomy_code_1)) |>
  dplyr::select(c(npi, dplyr::num_range("provider_license_number_state_code_", 1:15))) |>
  tidyr::pivot_longer(cols = dplyr::contains("state_code"),
                      names_to = "id_set",
                      values_to = "license_state",
                      values_drop_na = TRUE,
                      names_prefix = "provider_license_number_state_code_")

nppes_licenses_3 <- nppes_txon |>
  dplyr::left_join(nppes_txon_license,
                   by = dplyr::join_by(npi, id_set)) |>
  dplyr::left_join(nppes_txon_state,
                   by = dplyr::join_by(npi, id_set)) |>
  dplyr::select(npi,
                id_set,
                license = provider_license,
                taxonomy = taxonomy_code,
                license_state) |>
  dplyr::filter(!is.na(license))

nppes_licenses_3 |> readr::write_csv(here("data", "nppes_licenses_3.csv"), progress = TRUE)

# -------------------------------------------------------------------------
nppes_licenses_4 <- readr::read_csv(
  here("data_raw/NPPES_Data_Dissemination_January_2023", "npidata_pfile_20050523-20230108.csv"),
  col_types = readr::cols(.default = readr::col_character()),
  col_names = nppes_cols,
  col_select = c(1, 48:107),
  n_max = 1000000,
  skip = 3000000)

nppes_txon <- nppes_licenses_4 |>
  dplyr::filter(!is.na(healthcare_provider_taxonomy_code_1)) |>
  dplyr::select(c(npi, dplyr::contains("taxonomy_code"))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "taxonomy_code",
                      names_prefix = "healthcare_provider_taxonomy_code_",
                      values_drop_na = TRUE)

nppes_txon_license <- nppes_licenses_4 |>
  dplyr::filter(!is.na(healthcare_provider_taxonomy_code_1)) |>
  dplyr::select(c(npi, dplyr::num_range("provider_license_number_", 1:15))) |>
  tidyr::pivot_longer(cols = dplyr::contains("license_number"),
                      names_to = "id_set",
                      values_to = "provider_license",
                      values_drop_na = TRUE,
                      names_prefix = "provider_license_number_")

nppes_txon_state <- nppes_licenses_4 |>
  dplyr::filter(!is.na(healthcare_provider_taxonomy_code_1)) |>
  dplyr::select(c(npi, dplyr::num_range("provider_license_number_state_code_", 1:15))) |>
  tidyr::pivot_longer(cols = dplyr::contains("state_code"),
                      names_to = "id_set",
                      values_to = "license_state",
                      values_drop_na = TRUE,
                      names_prefix = "provider_license_number_state_code_")

nppes_licenses_4 <- nppes_txon |>
  dplyr::left_join(nppes_txon_license,
                   by = dplyr::join_by(npi, id_set)) |>
  dplyr::left_join(nppes_txon_state,
                   by = dplyr::join_by(npi, id_set)) |>
  dplyr::select(npi,
                id_set,
                license = provider_license,
                taxonomy = taxonomy_code,
                license_state) |>
  dplyr::filter(!is.na(license))

nppes_licenses_4 |> readr::write_csv(here("data", "nppes_licenses_4.csv"), progress = TRUE)

# -------------------------------------------------------------------------
nppes_licenses_5 <- readr::read_csv(
  here("data_raw/NPPES_Data_Dissemination_January_2023", "npidata_pfile_20050523-20230108.csv"),
  col_types = readr::cols(.default = readr::col_character()),
  col_names = nppes_cols,
  col_select = c(1, 48:107),
  n_max = 1000000,
  skip = 4000000)

nppes_txon <- nppes_licenses_5 |>
  dplyr::filter(!is.na(healthcare_provider_taxonomy_code_1)) |>
  dplyr::select(c(npi, dplyr::contains("taxonomy_code"))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "taxonomy_code",
                      names_prefix = "healthcare_provider_taxonomy_code_",
                      values_drop_na = TRUE)

nppes_txon_license <- nppes_licenses_5 |>
  dplyr::filter(!is.na(healthcare_provider_taxonomy_code_1)) |>
  dplyr::select(c(npi, dplyr::num_range("provider_license_number_", 1:15))) |>
  tidyr::pivot_longer(cols = dplyr::contains("license_number"),
                      names_to = "id_set",
                      values_to = "provider_license",
                      values_drop_na = TRUE,
                      names_prefix = "provider_license_number_")

nppes_txon_state <- nppes_licenses_5 |>
  dplyr::filter(!is.na(healthcare_provider_taxonomy_code_1)) |>
  dplyr::select(c(npi, dplyr::num_range("provider_license_number_state_code_", 1:15))) |>
  tidyr::pivot_longer(cols = dplyr::contains("state_code"),
                      names_to = "id_set",
                      values_to = "license_state",
                      values_drop_na = TRUE,
                      names_prefix = "provider_license_number_state_code_")

nppes_licenses_5 <- nppes_txon |>
  dplyr::left_join(nppes_txon_license,
                   by = dplyr::join_by(npi, id_set)) |>
  dplyr::left_join(nppes_txon_state,
                   by = dplyr::join_by(npi, id_set)) |>
  dplyr::select(npi,
                id_set,
                license = provider_license,
                taxonomy = taxonomy_code,
                license_state) |>
  dplyr::filter(!is.na(license))

nppes_licenses_5 |> readr::write_csv(here("data", "nppes_licenses_5.csv"), progress = TRUE)

# -------------------------------------------------------------------------
nppes_licenses_6 <- readr::read_csv(
  here("data_raw/NPPES_Data_Dissemination_January_2023", "npidata_pfile_20050523-20230108.csv"),
  col_types = readr::cols(.default = readr::col_character()),
  col_names = nppes_cols,
  col_select = c(1, 48:107),
  n_max = 1000000,
  skip = 5000000)

nppes_txon <- nppes_licenses_6 |>
  dplyr::filter(!is.na(healthcare_provider_taxonomy_code_1)) |>
  dplyr::select(c(npi, dplyr::contains("taxonomy_code"))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "taxonomy_code",
                      names_prefix = "healthcare_provider_taxonomy_code_",
                      values_drop_na = TRUE)

nppes_txon_license <- nppes_licenses_6 |>
  dplyr::filter(!is.na(healthcare_provider_taxonomy_code_1)) |>
  dplyr::select(c(npi, dplyr::num_range("provider_license_number_", 1:15))) |>
  tidyr::pivot_longer(cols = dplyr::contains("license_number"),
                      names_to = "id_set",
                      values_to = "provider_license",
                      values_drop_na = TRUE,
                      names_prefix = "provider_license_number_")

nppes_txon_state <- nppes_licenses_6 |>
  dplyr::filter(!is.na(healthcare_provider_taxonomy_code_1)) |>
  dplyr::select(c(npi, dplyr::num_range("provider_license_number_state_code_", 1:15))) |>
  tidyr::pivot_longer(cols = dplyr::contains("state_code"),
                      names_to = "id_set",
                      values_to = "license_state",
                      values_drop_na = TRUE,
                      names_prefix = "provider_license_number_state_code_")

nppes_licenses_6 <- nppes_txon |>
  dplyr::left_join(nppes_txon_license,
                   by = dplyr::join_by(npi, id_set)) |>
  dplyr::left_join(nppes_txon_state,
                   by = dplyr::join_by(npi, id_set)) |>
  dplyr::select(npi,
                id_set,
                license = provider_license,
                taxonomy = taxonomy_code,
                license_state) |>
  dplyr::filter(!is.na(license))

nppes_licenses_6 |> readr::write_csv(here("data", "nppes_licenses_6.csv"), progress = TRUE)

# -------------------------------------------------------------------------
nppes_licenses_7 <- readr::read_csv(
  here("data_raw/NPPES_Data_Dissemination_January_2023", "npidata_pfile_20050523-20230108.csv"),
  col_types = readr::cols(.default = readr::col_character()),
  col_names = nppes_cols,
  col_select = c(1, 48:107),
  n_max = 1000000,
  skip = 6000000)

nppes_txon <- nppes_licenses_7 |>
  dplyr::filter(!is.na(healthcare_provider_taxonomy_code_1)) |>
  dplyr::select(c(npi, dplyr::contains("taxonomy_code"))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "taxonomy_code",
                      names_prefix = "healthcare_provider_taxonomy_code_",
                      values_drop_na = TRUE)

nppes_txon_license <- nppes_licenses_7 |>
  dplyr::filter(!is.na(healthcare_provider_taxonomy_code_1)) |>
  dplyr::select(c(npi, dplyr::num_range("provider_license_number_", 1:15))) |>
  tidyr::pivot_longer(cols = dplyr::contains("license_number"),
                      names_to = "id_set",
                      values_to = "provider_license",
                      values_drop_na = TRUE,
                      names_prefix = "provider_license_number_")

nppes_txon_state <- nppes_licenses_7 |>
  dplyr::filter(!is.na(healthcare_provider_taxonomy_code_1)) |>
  dplyr::select(c(npi, dplyr::num_range("provider_license_number_state_code_", 1:15))) |>
  tidyr::pivot_longer(cols = dplyr::contains("state_code"),
                      names_to = "id_set",
                      values_to = "license_state",
                      values_drop_na = TRUE,
                      names_prefix = "provider_license_number_state_code_")

nppes_licenses_7 <- nppes_txon |>
  dplyr::left_join(nppes_txon_license,
                   by = dplyr::join_by(npi, id_set)) |>
  dplyr::left_join(nppes_txon_state,
                   by = dplyr::join_by(npi, id_set)) |>
  dplyr::select(npi,
                id_set,
                license = provider_license,
                taxonomy = taxonomy_code,
                license_state) |>
  dplyr::filter(!is.na(license))

nppes_licenses_7 |> readr::write_csv(here("data", "nppes_licenses_7.csv"), progress = TRUE)

# -------------------------------------------------------------------------
nppes_licenses_8 <- readr::read_csv(
  here("data_raw/NPPES_Data_Dissemination_January_2023", "npidata_pfile_20050523-20230108.csv"),
  col_types = readr::cols(.default = readr::col_character()),
  col_names = nppes_cols,
  col_select = c(1, 48:107),
  n_max = 1000000,
  skip = 7000000)

nppes_txon <- nppes_licenses_8 |>
  dplyr::filter(!is.na(healthcare_provider_taxonomy_code_1)) |>
  dplyr::select(c(npi, dplyr::contains("taxonomy_code"))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "taxonomy_code",
                      names_prefix = "healthcare_provider_taxonomy_code_",
                      values_drop_na = TRUE)

nppes_txon_license <- nppes_licenses_8 |>
  dplyr::filter(!is.na(healthcare_provider_taxonomy_code_1)) |>
  dplyr::select(c(npi, dplyr::num_range("provider_license_number_", 1:15))) |>
  tidyr::pivot_longer(cols = dplyr::contains("license_number"),
                      names_to = "id_set",
                      values_to = "provider_license",
                      values_drop_na = TRUE,
                      names_prefix = "provider_license_number_")

nppes_txon_state <- nppes_licenses_8 |>
  dplyr::filter(!is.na(healthcare_provider_taxonomy_code_1)) |>
  dplyr::select(c(npi, dplyr::num_range("provider_license_number_state_code_", 1:15))) |>
  tidyr::pivot_longer(cols = dplyr::contains("state_code"),
                      names_to = "id_set",
                      values_to = "license_state",
                      values_drop_na = TRUE,
                      names_prefix = "provider_license_number_state_code_")

nppes_licenses_8 <- nppes_txon |>
  dplyr::left_join(nppes_txon_license,
                   by = dplyr::join_by(npi, id_set)) |>
  dplyr::left_join(nppes_txon_state,
                   by = dplyr::join_by(npi, id_set)) |>
  dplyr::select(npi,
                id_set,
                license = provider_license,
                taxonomy = taxonomy_code,
                license_state) |>
  dplyr::filter(!is.na(license))

nppes_licenses_8 |> readr::write_csv(here("data", "nppes_licenses_8.csv"), progress = TRUE)

# -------------------------------------------------------------------------
# Read from multiple file paths at once
fnums <- 1:8
files <- paste0("nppes_licenses_", fnums, ".csv")
nppes_licenses_bind <- readr::read_csv(
  here("data", files),
  col_types = readr::cols(.default = readr::col_character()))

nppes_licenses_bind |> readr::write_csv(here("data", "nppes_licenses_bind.csv"), progress = TRUE)

# -------------------------------------------------------------------------
fs::dir_info(recurse = TRUE) |>
  dplyr::filter(type == "file") |> print(n = 29)
dplyr::summarise(n = dplyr::n(), size = sum(size))

fs::dir_ls("data/", recurse = TRUE)

dims <- glue::glue(
  "{ds |> names() |> length()} cols by ",
  "{scales::label_number(big.mark = ',')(ds |> count() |> collect() |> pull())} rows"
)



