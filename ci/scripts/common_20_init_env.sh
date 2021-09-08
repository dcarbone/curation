#!/usr/bin/env bash

set -e

# just in case we have JIT-defined envvars
source "${HOME}/.bashrc"
source "${HOME}/.profile"

# import funcs
source "${CIRCLE_WORKING_DIRECTORY}"/ci/scripts/funcs.sh

set_env "PROJECT_USERNAME" "${CIRCLE_PROJECT_USERNAME//-/_}"

# determine username
determined_username=$(define_username)
set_env "USERNAME" "${determined_username}"
set_env "USERNAME_PREFIX" "${USERNAME//-/_}"
set_env "CIRCLE_PROJECT_USERNAME" "all-of-us"

# remove newlines from last commit message
cleaned_last_commit=$(escape_newlines "$(git log -1 --pretty=%B)")
set_env "GIT_LAST_LOG" "${cleaned_last_commit}" 1

# reformat local branch name
cleaned_circle_branch=$(underscore_me "$(git rev-parse --abbrev-ref HEAD)")
set_env "CIRCLE_BRANCH" "${cleaned_circle_branch}" 1

set_env "CURRENT_BRANCH" "${CIRCLE_BRANCH}"
set_env "GOOGLE_APPLICATION_CREDENTIALS" "${HOME}/gcloud-credentials-key.json"
set_env "APPLICATION_ID" "aou-res-curation-test"
set_env "GOOGLE_CLOUD_PROJECT" "${APPLICATION_ID}"
set_env "PROJECT_PREFIX" "${APPLICATION_ID//-/_}"

## dataset envvars

set_env "DATASET_PREFIX" "${PROJECT_USERNAME}_${USERNAME_PREFIX}_${CURRENT_BRANCH}"

set_env "BIGQUERY_DATASET_ID" "${DATASET_PREFIX}"_ehr
set_env "RDR_DATASET_ID" "${DATASET_PREFIX}"_rdr
set_env "EHR_RDR_DATASET_ID" "${DATASET_PREFIX}"_ehr_rdr
set_env "COMBINED_DATASET_ID" "${DATASET_PREFIX}"_combined
set_env "UNIONED_DATASET_ID" "${DATASET_PREFIX}"_unioned
set_env "COMBINED_DEID_DATASET_ID" "${DATASET_PREFIX}"_deid
set_env "FITBIT_DATSET_ID" "${DATASET_PREFIX}"_fitbit
set_env "VOCABULARY_DATASET" "vocabulary20210601"

## bucket envvars

set_env "BUCKET_PREFIX" "${PROJECT_USERNAME}"_"${USERNAME_PREFIX}"_"${CURRENT_BRANCH}"

set_env "DRC_BUCKET_NAME" "${BUCKET_PREFIX}"_drc
set_env "BUCKET_NAME_FAKE" "${BUCKET_PREFIX}"_fake
set_env "BUCKET_NAME_NYC" "${BUCKET_PREFIX}"_nyc
set_env "BUCKET_NAME_PITT" "${BUCKET_PREFIX}"_pitt
set_env "BUCKET_NAME_CHS" "${BUCKET_PREFIX}"_chs
set_env "BUCKET_NAME_UNIONED_EHR" "${BUCKET_PREFIX}"_unioned_ehr
set_env "BUCKET_NAME_${BUCKET_PREFIX}_FAKE" "${BUCKET_NAME_FAKE}"
