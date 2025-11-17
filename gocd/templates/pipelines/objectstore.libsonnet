local utils = import '../libs/utils.libsonnet';
local gocdtasks = import 'github.com/getsentry/gocd-jsonnet/libs/gocd-tasks.libsonnet';

local deploy_canary(region) = [
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
            PAUSE_MESSAGE: 'Pausing pipeline due to canary failure.',
          },
          tasks: [
            gocdtasks.script(importstr '../bash/deploy.sh'),
            gocdtasks.script(importstr '../bash/wait-canary.sh'),
            // TODO: Add sentry error checks
            // TODO: Add datadog monitors
            utils.pause_on_failure(),
          ],
        },
      },
    },
  },
];

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
  stages: utils.github_checks() + deploy_canary(region) + deploy_primary(region),
}
