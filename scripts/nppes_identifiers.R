

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
nppes_identifiers_1 <- readr::read_csv(
  here("data_raw/NPPES_Data_Dissemination_January_2023", "npidata_pfile_20050523-20230108.csv"),
  col_types = readr::cols(.default = readr::col_character()),
  col_select = c(1, 108:306),
  n_max = 1000000,
  skip = 0,
  name_repair = janitor::make_clean_names)

nppes_ids <- nppes_identifiers_1 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "identifier",
                      names_prefix = "other_provider_identifier_",
                      values_drop_na = TRUE)

nppes_type_codes <- nppes_identifiers_1 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_type_code_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "type_code",
                      names_prefix = "other_provider_identifier_type_code_",
                      values_drop_na = TRUE)

nppes_id_states <- nppes_identifiers_1 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_state_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "id_state",
                      names_prefix = "other_provider_identifier_state_",
                      values_drop_na = TRUE)

nppes_id_issuers <- nppes_identifiers_1 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_issuer_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "issuer",
                      names_prefix = "other_provider_identifier_issuer_",
                      values_drop_na = TRUE)

nppes_identifiers_1 <- nppes_ids |>
  dplyr::left_join(nppes_type_codes, by = dplyr::join_by(npi, id_set)) |>
  dplyr::left_join(nppes_id_states, by = dplyr::join_by(npi, id_set)) |>
  dplyr::left_join(nppes_id_issuers, by = dplyr::join_by(npi, id_set)) |>
  dplyr::select(npi,
                id_set,
                identifier,
                id_state,
                id_code = type_code) |>
  dplyr::filter(!is.na(identifier))

nppes_identifiers_1 |> readr::write_csv(here("data", "nppes_identifiers_1.csv"), progress = TRUE)

# -------------------------------------------------------------------------
nppes_identifiers_2 <- readr::read_csv(
  here("data_raw/NPPES_Data_Dissemination_January_2023", "npidata_pfile_20050523-20230108.csv"),
  col_types = readr::cols(.default = readr::col_character()),
  col_names = nppes_cols,
  col_select = c(1, 108:306),
  n_max = 1000000,
  skip = 1000000)

nppes_ids <- nppes_identifiers_2 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "identifier",
                      names_prefix = "other_provider_identifier_",
                      values_drop_na = TRUE)

nppes_type_codes <- nppes_identifiers_2 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_type_code_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "type_code",
                      names_prefix = "other_provider_identifier_type_code_",
                      values_drop_na = TRUE)

nppes_id_states <- nppes_identifiers_2 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_state_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "id_state",
                      names_prefix = "other_provider_identifier_state_",
                      values_drop_na = TRUE)

nppes_id_issuers <- nppes_identifiers_2 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_issuer_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "issuer",
                      names_prefix = "other_provider_identifier_issuer_",
                      values_drop_na = TRUE)

nppes_identifiers_2 <- nppes_ids |>
  dplyr::left_join(nppes_type_codes, by = dplyr::join_by(npi, id_set)) |>
  dplyr::left_join(nppes_id_states, by = dplyr::join_by(npi, id_set)) |>
  dplyr::left_join(nppes_id_issuers, by = dplyr::join_by(npi, id_set)) |>
  dplyr::select(npi,
                id_set,
                identifier,
                id_state,
                id_code = type_code) |>
  dplyr::filter(!is.na(identifier))

nppes_identifiers_2 |> readr::write_csv(here("data", "nppes_identifiers_2.csv"), progress = TRUE)

# -------------------------------------------------------------------------
nppes_identifiers_3 <- readr::read_csv(
  here("data_raw/NPPES_Data_Dissemination_January_2023", "npidata_pfile_20050523-20230108.csv"),
  col_types = readr::cols(.default = readr::col_character()),
  col_names = nppes_cols,
  col_select = c(1, 108:306),
  n_max = 1000000,
  skip = 2000000)

nppes_ids <- nppes_identifiers_3 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "identifier",
                      names_prefix = "other_provider_identifier_",
                      values_drop_na = TRUE)

nppes_type_codes <- nppes_identifiers_3 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_type_code_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "type_code",
                      names_prefix = "other_provider_identifier_type_code_",
                      values_drop_na = TRUE)

nppes_id_states <- nppes_identifiers_3 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_state_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "id_state",
                      names_prefix = "other_provider_identifier_state_",
                      values_drop_na = TRUE)

nppes_id_issuers <- nppes_identifiers_3 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_issuer_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "issuer",
                      names_prefix = "other_provider_identifier_issuer_",
                      values_drop_na = TRUE)

nppes_identifiers_3 <- nppes_ids |>
  dplyr::left_join(nppes_type_codes, by = dplyr::join_by(npi, id_set)) |>
  dplyr::left_join(nppes_id_states, by = dplyr::join_by(npi, id_set)) |>
  dplyr::left_join(nppes_id_issuers, by = dplyr::join_by(npi, id_set)) |>
  dplyr::select(npi,
                id_set,
                identifier,
                id_state,
                id_code = type_code) |>
  dplyr::filter(!is.na(identifier))

nppes_identifiers_3 |> readr::write_csv(here("data", "nppes_identifiers_3.csv"), progress = TRUE)

# -------------------------------------------------------------------------
nppes_identifiers_4 <- readr::read_csv(
  here("data_raw/NPPES_Data_Dissemination_January_2023", "npidata_pfile_20050523-20230108.csv"),
  col_types = readr::cols(.default = readr::col_character()),
  col_names = nppes_cols,
  col_select = c(1, 108:306),
  n_max = 1000000,
  skip = 3000000)

nppes_ids <- nppes_identifiers_4 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "identifier",
                      names_prefix = "other_provider_identifier_",
                      values_drop_na = TRUE)

nppes_type_codes <- nppes_identifiers_4 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_type_code_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "type_code",
                      names_prefix = "other_provider_identifier_type_code_",
                      values_drop_na = TRUE)

nppes_id_states <- nppes_identifiers_4 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_state_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "id_state",
                      names_prefix = "other_provider_identifier_state_",
                      values_drop_na = TRUE)

nppes_id_issuers <- nppes_identifiers_4 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_issuer_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "issuer",
                      names_prefix = "other_provider_identifier_issuer_",
                      values_drop_na = TRUE)

nppes_identifiers_4 <- nppes_ids |>
  dplyr::left_join(nppes_type_codes, by = dplyr::join_by(npi, id_set)) |>
  dplyr::left_join(nppes_id_states, by = dplyr::join_by(npi, id_set)) |>
  dplyr::left_join(nppes_id_issuers, by = dplyr::join_by(npi, id_set)) |>
  dplyr::select(npi,
                id_set,
                identifier,
                id_state,
                id_code = type_code) |>
  dplyr::filter(!is.na(identifier))

nppes_identifiers_4 |> readr::write_csv(here("data", "nppes_identifiers_4.csv"), progress = TRUE)

# -------------------------------------------------------------------------
nppes_identifiers_5 <- readr::read_csv(
  here("data_raw/NPPES_Data_Dissemination_January_2023", "npidata_pfile_20050523-20230108.csv"),
  col_types = readr::cols(.default = readr::col_character()),
  col_names = nppes_cols,
  col_select = c(1, 108:306),
  n_max = 1000000,
  skip = 4000000)

nppes_ids <- nppes_identifiers_5 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "identifier",
                      names_prefix = "other_provider_identifier_",
                      values_drop_na = TRUE)

nppes_type_codes <- nppes_identifiers_5 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_type_code_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "type_code",
                      names_prefix = "other_provider_identifier_type_code_",
                      values_drop_na = TRUE)

nppes_id_states <- nppes_identifiers_5 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_state_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "id_state",
                      names_prefix = "other_provider_identifier_state_",
                      values_drop_na = TRUE)

nppes_id_issuers <- nppes_identifiers_5 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_issuer_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "issuer",
                      names_prefix = "other_provider_identifier_issuer_",
                      values_drop_na = TRUE)

nppes_identifiers_5 <- nppes_ids |>
  dplyr::left_join(nppes_type_codes, by = dplyr::join_by(npi, id_set)) |>
  dplyr::left_join(nppes_id_states, by = dplyr::join_by(npi, id_set)) |>
  dplyr::left_join(nppes_id_issuers, by = dplyr::join_by(npi, id_set)) |>
  dplyr::select(npi,
                id_set,
                identifier,
                id_state,
                id_code = type_code) |>
  dplyr::filter(!is.na(identifier))

nppes_identifiers_5 |> readr::write_csv(here("data", "nppes_identifiers_5.csv"), progress = TRUE)

# -------------------------------------------------------------------------
nppes_identifiers_6 <- readr::read_csv(
  here("data_raw/NPPES_Data_Dissemination_January_2023", "npidata_pfile_20050523-20230108.csv"),
  col_types = readr::cols(.default = readr::col_character()),
  col_names = nppes_cols,
  col_select = c(1, 108:306),
  n_max = 1000000,
  skip = 5000000)

nppes_ids <- nppes_identifiers_6 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "identifier",
                      names_prefix = "other_provider_identifier_",
                      values_drop_na = TRUE)

nppes_type_codes <- nppes_identifiers_6 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_type_code_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "type_code",
                      names_prefix = "other_provider_identifier_type_code_",
                      values_drop_na = TRUE)

nppes_id_states <- nppes_identifiers_6 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_state_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "id_state",
                      names_prefix = "other_provider_identifier_state_",
                      values_drop_na = TRUE)

nppes_id_issuers <- nppes_identifiers_6 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_issuer_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "issuer",
                      names_prefix = "other_provider_identifier_issuer_",
                      values_drop_na = TRUE)

nppes_identifiers_6 <- nppes_ids |>
  dplyr::left_join(nppes_type_codes, by = dplyr::join_by(npi, id_set)) |>
  dplyr::left_join(nppes_id_states, by = dplyr::join_by(npi, id_set)) |>
  dplyr::left_join(nppes_id_issuers, by = dplyr::join_by(npi, id_set)) |>
  dplyr::select(npi,
                id_set,
                identifier,
                id_state,
                id_code = type_code) |>
  dplyr::filter(!is.na(identifier))

nppes_identifiers_6 |> readr::write_csv(here("data", "nppes_identifiers_6.csv"), progress = TRUE)

# -------------------------------------------------------------------------
nppes_identifiers_7 <- readr::read_csv(
  here("data_raw/NPPES_Data_Dissemination_January_2023", "npidata_pfile_20050523-20230108.csv"),
  col_types = readr::cols(.default = readr::col_character()),
  col_names = nppes_cols,
  col_select = c(1, 108:306),
  n_max = 1000000,
  skip = 6000000)

nppes_ids <- nppes_identifiers_7 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "identifier",
                      names_prefix = "other_provider_identifier_",
                      values_drop_na = TRUE)

nppes_type_codes <- nppes_identifiers_7 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_type_code_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "type_code",
                      names_prefix = "other_provider_identifier_type_code_",
                      values_drop_na = TRUE)

nppes_id_states <- nppes_identifiers_7 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_state_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "id_state",
                      names_prefix = "other_provider_identifier_state_",
                      values_drop_na = TRUE)

nppes_id_issuers <- nppes_identifiers_7 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_issuer_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "issuer",
                      names_prefix = "other_provider_identifier_issuer_",
                      values_drop_na = TRUE)

nppes_identifiers_7 <- nppes_ids |>
  dplyr::left_join(nppes_type_codes, by = dplyr::join_by(npi, id_set)) |>
  dplyr::left_join(nppes_id_states, by = dplyr::join_by(npi, id_set)) |>
  dplyr::left_join(nppes_id_issuers, by = dplyr::join_by(npi, id_set)) |>
  dplyr::select(npi,
                id_set,
                identifier,
                id_state,
                id_code = type_code) |>
  dplyr::filter(!is.na(identifier))

nppes_identifiers_7 |> readr::write_csv(here("data", "nppes_identifiers_7.csv"), progress = TRUE)

# -------------------------------------------------------------------------
nppes_identifiers_8 <- readr::read_csv(
  here("data_raw/NPPES_Data_Dissemination_January_2023", "npidata_pfile_20050523-20230108.csv"),
  col_types = readr::cols(.default = readr::col_character()),
  col_names = nppes_cols,
  col_select = c(1, 108:306),
  n_max = 1000000,
  skip = 7000000)

nppes_ids <- nppes_identifiers_8 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "identifier",
                      names_prefix = "other_provider_identifier_",
                      values_drop_na = TRUE)

nppes_type_codes <- nppes_identifiers_8 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_type_code_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "type_code",
                      names_prefix = "other_provider_identifier_type_code_",
                      values_drop_na = TRUE)

nppes_id_states <- nppes_identifiers_8 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_state_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "id_state",
                      names_prefix = "other_provider_identifier_state_",
                      values_drop_na = TRUE)

nppes_id_issuers <- nppes_identifiers_8 |>
  dplyr::filter(!is.na(other_provider_identifier_1)) |>
  dplyr::select(c(npi, dplyr::num_range("other_provider_identifier_issuer_", 1:50))) |>
  tidyr::pivot_longer(!npi,
                      names_to = "id_set",
                      values_to = "issuer",
                      names_prefix = "other_provider_identifier_issuer_",
                      values_drop_na = TRUE)

nppes_identifiers_8 <- nppes_ids |>
  dplyr::left_join(nppes_type_codes, by = dplyr::join_by(npi, id_set)) |>
  dplyr::left_join(nppes_id_states, by = dplyr::join_by(npi, id_set)) |>
  dplyr::left_join(nppes_id_issuers, by = dplyr::join_by(npi, id_set)) |>
  dplyr::select(npi,
                id_set,
                identifier,
                id_state,
                id_code = type_code) |>
  dplyr::filter(!is.na(identifier))

nppes_identifiers_8 |> readr::write_csv(here("data", "nppes_identifiers_8.csv"), progress = TRUE)

# -------------------------------------------------------------------------
# Read from multiple file paths at once
fnums <- 1:8
files <- paste0("nppes_identifiers_", fnums, ".csv")
nppes_identifiers_bind <- readr::read_csv(
  here("data", files),
  col_types = readr::cols(.default = readr::col_character()))

nppes_identifiers_bind |> readr::write_csv(here("data", "nppes_identifiers_bind.csv"), progress = TRUE)



