// Learn more about GoCD Pipedream here:
// https://www.notion.so/sentry/Pipedreams-in-GoCD-with-Jsonnet-430f46b87fa14650a80adf6708b088d9

local objectstore = import './pipelines/objectstore.libsonnet';
local pipedream = import 'github.com/getsentry/gocd-jsonnet/libs/pipedream.libsonnet';

local pipedream_config = {
  name: 'objectstore',
  auto_deploy: false,
  materials: {
    relay_repo: {
      git: 'git@github.com:getsentry/objectstore.git',
      shallow_clone: true,
      branch: 'main',
      destination: 'objectstore',
    },
  },
  rollback: {
    material_name: 'objectstore_repo',
    stage: 'deploy-primary',
    elastic_profile_id: 'objectstore',
  },
};

pipedream.render(pipedream_config, objectstore)
