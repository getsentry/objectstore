local utils = import '../libs/utils.libsonnet';
local gocdtasks = import 'github.com/getsentry/gocd-jsonnet/libs/gocd-tasks.libsonnet';

local deploy_primary(region) = [
  {
    'deploy-primary': {
      fetch_materials: true,
      jobs: {
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
    relay_repo: {
      git: 'git@github.com:getsentry/objectstore.git',
      shallow_clone: true,
      branch: 'main',
      destination: 'objectstore',
    },
  },
  stages: utils.github_checks() + deploy_primary(region),
}
