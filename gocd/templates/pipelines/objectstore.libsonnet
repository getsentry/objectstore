local utils = import '../libs/utils.libsonnet';
local gocdtasks = import 'github.com/getsentry/gocd-jsonnet/libs/gocd-tasks.libsonnet';

// NOTE: Sync with objectstore-k8s in getsentry/ops.
local region_has_canary(region) =
  region == 'de' || region == 'us';

local soak_job(region, time_mins) =
  {
    timeout: 60 * time_mins + 30,  // soak time + buffer
    elastic_profile_id: 'objectstore',
    environment_variables: {
      GOCD_ACCESS_TOKEN: '{{SECRET:[devinfra][gocd_access_token]}}',
      DATADOG_API_KEY: '{{SECRET:[devinfra][sentry_datadog_api_key]}}',
      DATADOG_APP_KEY: '{{SECRET:[devinfra][sentry_datadog_app_key]}}',
      SENTRY_AUTH_TOKEN: '{{SECRET:[devinfra-sentryio][token]}}',
      SENTRY_ENVIRONMENT: region,
      SOAK_TIME: time_mins,  // 1 minute
      ERROR_LIMIT: 1 * (time_mins * 60),  // 1 per second
      DRY_RUN: 'false',
      SKIP_CANARY_CHECKS: 'false',
      PAUSE_MESSAGE: 'Pausing pipeline due to canary failure.',
    },
    tasks: [
      gocdtasks.script(importstr '../bash/wait.sh'),
      gocdtasks.script(importstr '../bash/check-sentry-errors.sh'),
      gocdtasks.script(importstr '../bash/check-sentry-new-errors.sh'),
      gocdtasks.script(importstr '../bash/check-datadog-monitors.sh'),
      utils.pause_on_failure(),
    ],
  };

local deploy_canary(region) =
  if region_has_canary(region) then
    [
      {
        'deploy-canary': {
          fetch_materials: true,
          jobs: {
            create_sentry_release: {
              environment_variables: {
                SENTRY_ORG: 'sentry',
                SENTRY_PROJECT: 'objectstore',
                SENTRY_AUTH_TOKEN: '{{SECRET:[devinfra-sentryio][token]}}',
                SENTRY_ENVIRONMENT: region + '-canary',
              },
              timeout: 60,
              elastic_profile_id: 'objectstore',
              tasks: [
                gocdtasks.script(importstr '../bash/create-sentry-release.sh'),
              ],
            },
            deploy: {
              timeout: 300,
              elastic_profile_id: 'objectstore',
              environment_variables: {
                K8S_ENVIRONMENT: 'canary',
              },
              tasks: [
                gocdtasks.script(importstr '../bash/deploy.sh'),
              ],
            },
            soak: soak_job(region, 1),
          },
        },
      },
    ]
  else
    [];

local soak_time(region) =
  if region == 's4s' then
    [
      {
        'soak-time': {
          jobs: {
            soak: soak_job(region, 1),
          },
        },
      },
    ]
  else
    [];

local deploy_primary(region) = [
  {
    'deploy-primary': {
      fetch_materials: true,
      jobs: {
        create_sentry_release: {
          environment_variables: {
            SENTRY_ORG: 'sentry',
            SENTRY_PROJECT: 'objectstore',
            SENTRY_AUTH_TOKEN: '{{SECRET:[devinfra-sentryio][token]}}',
            SENTRY_ENVIRONMENT: region,
          },
          timeout: 60,
          elastic_profile_id: 'objectstore',
          tasks: [
            gocdtasks.script(importstr '../bash/create-sentry-release.sh'),
          ],
        },
        deploy: {
          timeout: 300,
          elastic_profile_id: 'objectstore',
          tasks: [
            gocdtasks.script(importstr '../bash/deploy.sh'),
          ],
        },
      },
    },
  },
];

function(region) {
  environment_variables: {
    SENTRY_REGION: region,
  },
  group: 'objectstore',
  lock_behavior: 'unlockWhenFinished',
  materials: {
    objectstore_repo: {
      git: 'git@github.com:getsentry/objectstore.git',
      shallow_clone: true,
      branch: 'main',
      destination: 'objectstore',
    },
  },
  stages: utils.github_checks() + deploy_canary(region) + deploy_primary(region) + soak_time(region),
}
